# internal pseudoclock device
# created April 2024 by Andi
# last change 29/5/2024 by Andi

import numpy as np
import labscript_utils.h5_lock
import h5py
from zprocess import Event
from zprocess.utils import _reraise

from labscript import LabscriptError
from labscript_utils import import_or_reload
from blacs.tab_base_classes import Worker

import logging
from user_devices.iPCdev.labscript_devices import (
    log_level,
    DEVICE_INFO_PATH, DEVICE_TIME, DEVICE_INFO_ADDRESS, DEVICE_INFO_TYPE, DEVICE_INFO_BOARD,
    DEVICE_DATA_AO, DEVICE_DATA_DO, DEVICE_DATA_DDS,
    HARDWARE_TYPE_AO, HARDWARE_TYPE_STATIC_AO, HARDWARE_TYPE_DO, HARDWARE_TYPE_STATIC_DO, HARDWARE_TYPE_TRG, HARDWARE_TYPE_DDS,
)

from time import sleep

# for testing
#from user_devices.h5_file_parser import read_group

# optional worker_args
ARG_SIM = 'simulate'

# default update time interval in seconds when status monitor shows actual status
UPDATE_TIME                     = 1.0

# default timeout in seconds for sync_boards
SYNC_TIMEOUT                    = 1.0

# time margin for sync_boards with reset_event_counter=True
SYNC_TIME_MARGIN                = 0.2

# events
EVENT_TO_PRIMARY                = '%s_to_prim'
EVENT_FROM_PRIMARY              = '%s_from_prim'
EVENT_TIMEOUT                   = 'timeout!'
EVENT_COUNT_INITIAL             = 0

# return code from sync_boards
SYNC_RESULT_OK                  = 0     # ok
SYNC_RESULT_TIMEOUT             = 1     # connection timeout
SYNC_RESULT_TIMEOUT_OTHER       = 2     # timeout on another board

class iPCdev_worker(Worker):
    def init(self):
        global zTimeoutError; from zprocess.utils import TimeoutError as zTimeoutError
        global get_ticks; from time import perf_counter as get_ticks
        global get_ticks; from time import sleep

        # reduce number of log entries in logfile (labscript-suite/logs/BLACS.log)
        self.logger.setLevel(log_level)
        #self.logger.setLevel(logging.INFO)

        self.worker_args = self.properties['worker_args']
        if ARG_SIM in self.worker_args:
            self.simulate = self.worker_args[ARG_SIM]
        else:
            self.simulate = False

        if self.simulate: print(self.device_name, 'init (simulate)\nworker args:', self.worker_args)
        else:             print(self.device_name, 'init\nworker args:', self.worker_args)

        # below we call static method extract_channel_data in a possibly derived class of iPCdev
        # dynamically load module and get the class object
        self.derived_module = self.properties['derived_module']
        print('derived module:', self.derived_module)
        print('device class:', self.device_class)
        device_module = import_or_reload(self.derived_module)
        self.device_class_object = getattr(device_module, self.device_class)

        if self.shared_clocklines:
            print('%i channels, %i clocklines (shared)' % (len(self.channels), len(self.clocklines)))
        else:
            print('%i channels' % (len(self.channels)))

        if not hasattr(self, 'update_time'):
            self.update_time = UPDATE_TIME

        # file id used to determine if file has changed or not
        self.file_id = None

        # experiment time in seconds and number of channels for different output types
        self.exp_time = 0
        self.num_channels = {}

        # prepare zprocess events for communication between primary and secondary boards
        # primary board: boards/events = list of all secondary board names/events
        # secondary board: boards/events = list containing only primary board name/event
        self.create_events()

        if False:
            # test events
            # sometimes after fresh start the events timeout without obvious reason!
            # here we test synchronization and if there is a timeout we re-create events.
            for i,event in enumerate(self.events):
                (timeout, result) = self.sync_boards(EVENT_STARTUP, payload=self.device_name)
                if timeout:
                    print('%s: sync test timeout! recreate events ...' % (self.device_name))
                else:
                    if self.is_primary:
                        boards = [self.device_name] + self.boards
                        for i, (board, name) in enumerate(result.items()):
                            if board != boards[i]:
                                raise LabscriptError("%s board %s expected but got %s!" % (self.device_name, boards[i], board))
                            elif board != name:
                                raise LabscriptError("%s board %s expected but got %s!" % (self.device_name, board, name))
                    else:
                        for board, name in result.items():
                            if board != name:
                                raise LabscriptError("%s board %s expected but got %s!" % (self.device_name, board, name))
                    print('%s: sync test result:' % (self.device_name), result)

    def create_events(self):
        if self.is_primary:
            self.events_wait = [self.process_tree.event(EVENT_TO_PRIMARY % self.device_name, role='wait')]
            self.events_post = [self.process_tree.event(EVENT_FROM_PRIMARY % s, role='post') for s in self.boards]
        else:
            self.events_post = [self.process_tree.event(EVENT_TO_PRIMARY % self.boards[0], role='post')]
            self.events_wait = [self.process_tree.event(EVENT_FROM_PRIMARY % self.device_name, role='wait')]
        # TODO: if initial count is zero get rarely timeout at start restart of tab
        #       adding an offset at restart MIGHT solve this issue but since it happens very rare I am not sure yet?
        self.event_count = EVENT_COUNT_INITIAL

    def sync_boards(self, payload=None, timeout=SYNC_TIMEOUT, reset_event_counter=False):
        # synchronize multiple boards
        # payload = data to be distributed to all boards.
        # timeout = timeout time in seconds
        # reset_event_counter = if True resets event counter before waiting.
        # 1. primary board waits for events of all secondary boards and then sends event back.
        # 2. each secondary board sends event to primary board and waits for primary event.
        # primary collects dictionary {board_name:payload} for all boards and sends back to all boards.
        # this allows to share data among boards.
        # returns (status, result, duration)
        # status   = SYNC_RESULT_OK if all ok
        #            SYNC_RESULT_TIMEOUT if connection timeout
        #            SYNC_RESULT_TIMEOUT_OTHER if connection to any other board timeout
        # result   = if not None dictionary with key = board name, value = payload
        # duration = total time in ms the worker spent in sync_boards function
        # timeout behaviour:
        # since each board can be reset by user self.event_count might get out of sync with other boards.
        # this will cause timeout on all boards - event the ones which are still synchronized!
        # this is by purpose to ensure all boards have the same waiting times.
        # on timeout each worker should call sync_boards again with reset_event_counter=True
        # this allows to re-synchronize all boards and continue without restarting of blacs.
        # note: in rare cases this was not working since the primary can by chance still wait for timeout,
        #       while any secondary is already timeout and calls sync_boards again but with reset self.event_count.
        #       in this case the primary might discard the new event since it still waits for the old event.
        #       when it then resets it will timeout because the new event was already discarded.
        #       to avoid this issue on reset_event_counter the secondary boards wait SYNC_TIME_MARGIN time
        #       before sending the reset events and the primary should be reset and waiting for the new event.
        t_start = get_ticks()
        if reset_event_counter: self.event_count = EVENT_COUNT_INITIAL
        sync_result = SYNC_RESULT_OK
        if self.is_primary:
            # 1. primary board: first wait then send
            if reset_event_counter:
                # compensate the additional waiting time of secondary boards
                timeout += SYNC_TIME_MARGIN
            result = {} if payload is None else {self.device_name:payload}
            event = self.events_wait[0]
            #sleep(0.1) # this triggers 100% the timeout event! when restarting both secondary boards!
            for i in range(len(self.boards)):
                is_timeout = False
                try:
                    _t_start = get_ticks()
                    #self.logger.log(logging.INFO, "%s (pri) wait evt %i (#%i) ..." % (self.device_name, self.event_count, i))
                    _result = event.wait(self.event_count, timeout=timeout/len(self.boards))
                    if _result is not None: result.update(_result)
                except zTimeoutError:
                    is_timeout = True
                    sync_result = SYNC_RESULT_TIMEOUT
                    result[self.boards[i]] = EVENT_TIMEOUT
                #self.logger.log(logging.WARNING if is_timeout else logging.INFO, "%s (pri) wait evt %i (#%i) %.3fms: %s" % (self.device_name, self.event_count, i, (get_ticks() - _t_start) * 1e3, 'timeout!' if is_timeout else str(_result)))
            if sync_result == SYNC_RESULT_OK:
                for event in self.events_post:
                    event.post(self.event_count, data=None if len(result) == 0 else result)
            else:
                # on timeout we have to ensure that primary board waits the same time as secondary boards,
                # otherwise primary board resets and starts waiting too early while other boards are still waiting for first event.
                remaining = timeout - (get_ticks() - t_start)
                if remaining > 0:
                    #self.logger.log(logging.INFO,"%s (pri) wait remaining %.3fms ..." % (self.device_name, remaining * 1e3))
                    sleep(remaining)
            # return total duration in ms
            duration = (get_ticks() - t_start) * 1e3
        else:
            # 2. secondary board: first send then wait
            if reset_event_counter:
                # ensure primary is reset before sending the reset event id
                sleep(SYNC_TIME_MARGIN)
            self.events_post[0].post(self.event_count, data={self.device_name:payload})
            is_timeout = False
            try:
                #self.logger.log(logging.INFO, "%s (sec) wait evt %i ..." % (self.device_name, self.event_count))
                result = self.events_wait[0].wait(self.event_count, timeout=timeout)
                if (result is not None) and (sync_result == SYNC_RESULT_OK):
                    for board, _result in result.items():
                        if isinstance(_result, str) and _result == EVENT_TIMEOUT:
                            sync_result = SYNC_RESULT_TIMEOUT_OTHER
                            break
            except zTimeoutError:
                is_timeout = True
                sync_result = SYNC_RESULT_TIMEOUT
                result = None
            duration = (get_ticks() - t_start) * 1e3
            #self.logger.log(logging.WARNING if is_timeout else logging.INFO, "%s (sec) wait evt %i %.3fms: %s" % (self.device_name, self.event_count, duration, 'timeout!' if is_timeout else 'ok'))
        self.event_count += 1

        return (sync_result, result, duration)

    def program_manual(self, front_panel_values):
        print(self.device_name, 'program manual')
        return {}

    def transition_to_buffered(self, device_name, h5file, initial_values, fresh):
        # this is called for all iPCdev devices
        print(self.device_name, 'transition to buffered')
        #print('initial values:', initial_values)
        final_values = {}
        update = False

        with h5py.File(h5file,'r') as f:
            # file id used to check if file has been changed
            id = f.attrs['sequence_id'] + ('_%i' % f.attrs['sequence_index']) + ('_%i' % f.attrs['run number'])
            if self.file_id is None or self.file_id != id:
                # new file
                self.file_id = id
                self.exp_time = 0
                self.num_channels = {}
                update = True

                # load data tables for all output channels
                for connection, device in self.channels.items():
                    hardware_type = device.hardware_info[DEVICE_INFO_TYPE]
                    group = f[device.hardware_info[DEVICE_INFO_PATH]]
                    times = group[DEVICE_TIME][()]
                    static = False
                    if hardware_type == HARDWARE_TYPE_AO:
                        devices = [(device.name, DEVICE_DATA_AO % (device.name, device.hardware_info[DEVICE_INFO_ADDRESS]),device.parent_port, 'AO')]
                    elif hardware_type == HARDWARE_TYPE_STATIC_AO:
                        devices = [(device.name, DEVICE_DATA_AO % (device.name, device.hardware_info[DEVICE_INFO_ADDRESS]),device.parent_port, None)]
                        static = True
                    elif (hardware_type == HARDWARE_TYPE_DO):
                        devices = [(device.name, DEVICE_DATA_DO % (device.hardware_info[DEVICE_INFO_BOARD], device.hardware_info[DEVICE_INFO_ADDRESS]), device.parent_port, 'DO')]
                    elif (hardware_type == HARDWARE_TYPE_STATIC_DO):
                        devices = [(device.name, DEVICE_DATA_DO % (device.hardware_info[DEVICE_INFO_BOARD], device.hardware_info[DEVICE_INFO_ADDRESS]), device.parent_port, None)]
                        static = True
                    elif (hardware_type == HARDWARE_TYPE_TRG):
                        devices = [(device.name, DEVICE_DATA_DO % (device.hardware_info[DEVICE_INFO_BOARD], device.hardware_info[DEVICE_INFO_ADDRESS]), device.parent_port, None)]
                    elif hardware_type == HARDWARE_TYPE_DDS:
                        devices = [(channel.name, DEVICE_DATA_DDS % (device.name, device.hardware_info[DEVICE_INFO_ADDRESS], channel.parent_port), channel.parent_port, None) for channel in device.child_list.values()]
                    else:
                        print("warning: device %s unknown type %s (skip)" % (device.name, hardware_type))
                        continue
                    final = {}
                    for (name, dataset, port, type) in devices:
                        data = group[dataset][()]
                        if data is None:
                            raise LabscriptError("device %s: dataset %s not existing!" % (name, dataset))
                        elif static and ((len(times) != 2) or (len(data) != 1)):
                            raise LabscriptError("static device %s: %i/%i times/data instead of 2/1!" % (name, len(times), len(data)))
                        elif not static and (len(times) != len(data)):
                            raise LabscriptError("device %s: %i times but %i data!" % (name, len(times), len(data)))
                        if times[-1] > self.exp_time: self.exp_time = times[-1]
                        channel_data = self.device_class_object.extract_channel_data(device.hardware_info, data)
                        final[port] = channel_data[-1]
                        # save number of used channels per type of port.
                        if (type is not None) and (len(channel_data) > 2):
                            changes = ((channel_data[1:].astype(int) - channel_data[:-1].astype(int)) != 0)
                            if np.any(changes):
                                try:
                                    self.num_channels[type] += 1
                                except KeyError:
                                    self.num_channels[type] = 1

                    if len(devices) == 1: final_values[connection] = final[device.parent_port]
                    else:                 final_values[connection] = final

        if   self.exp_time >= 1.0: tmp = '%.3f s'  % (self.exp_time)
        elif self.exp_time > 1e-3: tmp = '%.3f ms' % (self.exp_time*1e3)
        elif self.exp_time > 1e-6: tmp = '%.3f us' % (self.exp_time*1e6)
        else:                      tmp = '%.1f ns' % (self.exp_time*1e9)
        print('\n%s start experiment: duration %s %s' % (self.device_name, tmp, '(old file)' if not update else '(new file)'))

        #print('final values:', final_values)

        if True:
            # synchronize boards and get experiment time for all of them
            # note: this is for demonstration and could be commented.
            (timeout, board_times, duration) = self.sync_boards(id=EVENT_TRANSITION_TO_BUFFERED, payload=self.exp_time)
            if timeout:
                print("\ntimeout sync with all boards!\n")
                return None
            print('board times:', board_times)

            if True:
                # we set experiment time to largest of all boards
                for board, exp_time in board_times.items():
                    if exp_time > self.exp_time:
                        print('%s update duration from board %s to %.3e s' % (self.device_name, board,exp_time))
                        self.exp_time = exp_time

        # manually call start_run from here
        self.start_run()

        return final_values

    def transition_to_manual(self, abort=False):
        # this is called for all iPCdev devices
        print(self.device_name, 'transition to manual')

        error = 0

        if abort:
            self.board_status = {}
            print('board status: ABORTED!')
        else:
            # get number of samples
            tmp = []
            for name, samples in self.num_channels.items():
                tmp += ['%s: %i' % (name, samples)]
            if len(tmp) > 0: print("%s done, active channels: %s (ok)" % (self.device_name, ', '.join(tmp)))
            else:            print("%s done, no active channels (ok)" % (self.device_name))

            # get status (error) of all boards
            (timeout, self.board_status) = self.sync_boards(EVENT_TRANSITION_TO_MANUAL, payload=error)
            if timeout:
                print("\ntimeout get status of all boards!\n")
                return True
            print('board status:', self.board_status)

        # return True = all ok
        return (error == 0)

    def abort_transition_to_buffered(self):
        print(self.device_name, 'transition to buffered abort')
        return self.transition_to_manual(abort=True)

    def abort_buffered(self):
        print(self.device_name, 'buffered abort')
        return self.transition_to_manual(abort=True)

    def start_run(self):
        # note: this is called manually from transition_to_buffered for all iPCdev devices
        #       since iPCdev_tab::start_run is called only for primary pseudoclock device!
        #print(self.device_name, 'start run')
        self.t_start = get_ticks()
        self.t_last  = -2*self.update_time

        # return True = ok
        return True

    def status_monitor(self, status_end):
        """
        this is called from DeviceTab::status_monitor during run to update status - but of primary board only!
        if status_end = True then this is called from DeviceTab::status_end.
        return True = end or error. False = running.
        when returns True:
        1. transition_to_manual is called for ALL boards where we get self.board_status of all boards.
        2. status_monitor is called again with status_end=True for primary board only
           and worker should return self.board_status with key = board name. value = error code. 0 = ok.
        """
        end = False
        run_time = get_ticks() - self.t_start
        if self.simulate:
            end = (run_time >= self.exp_time)
        else:
            # TODO: implement for your device!
            end = (run_time >= self.exp_time)

        if end:
            if status_end:
                print(self.device_name, 'status monitor %.1f s (end - manual)' % run_time)
                end = self.board_status
            else:
                print(self.device_name, 'status monitor %.1f s (end)' % run_time)
        elif (run_time - self.t_last) >= self.update_time:
            self.t_last = run_time
            if status_end:
                print(self.device_name, 'status monitor %.1f s (aborted)' % run_time)
                end = self.board_status
            else:
                print(self.device_name, 'status monitor %.1f s (running)' % run_time)
        return end

    def restart(self):
        # restart tab only. return True = restart, False = do not restart.
        print(self.device_name, 'restart')
        # TODO: cleanup resources here
        # short sleep to allow user to read that we have cleaned up.
        sleep(0.5)
        return True

    def shutdown(self):
        # shutdown blacs
        print(self.device_name, 'shutdown')
        # TODO: cleanup resources here...
        # short sleep to allow user to read that we have cleaned up.
        sleep(0.5)
    
