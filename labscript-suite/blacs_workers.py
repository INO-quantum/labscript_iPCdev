import numpy as np
import labscript_utils.h5_lock
import h5py
from zprocess import Event
from zprocess.utils import _reraise

from labscript import LabscriptError
from labscript_utils import import_or_reload
from blacs.tab_base_classes import Worker

from time import sleep
import logging
from user_devices.iPCdev.labscript_devices import (
    log_level,
    DEVICE_INFO_PATH, DEVICE_TIME, DEVICE_INFO_ADDRESS, DEVICE_INFO_TYPE, DEVICE_INFO_BOARD,
    DEVICE_DATA_AO, DEVICE_DATA_DO, DEVICE_DATA_DDS,
    HARDWARE_TYPE_AO, HARDWARE_TYPE_STATIC_AO, HARDWARE_TYPE_DO, HARDWARE_TYPE_STATIC_DO, HARDWARE_TYPE_TRG, HARDWARE_TYPE_DDS,
)
# for testing
#from user_devices.h5_file_parser import read_group

# optional worker_args
ARG_SIM = 'simulate'

# default update time interval in seconds when status monitor shows actual status
UPDATE_TIME = 1.0

# event ID's in steps of 2
EVENT_TRANSITION_TO_BUFFERED    = 2
EVENT_TRANSITION_TO_MANUAL      = 4

class iPCdev_worker(Worker):
    def init(self):
        global zTimeoutError; from zprocess.utils import TimeoutError as zTimeoutError
        global get_ticks; from time import perf_counter as get_ticks
        global get_ticks; from time import sleep

        # reduce number of log entries in logfile (labscript-suite/logs/BLACS.log)
        self.logger.setLevel(log_level)

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

        # experiment time in seconds and number of samples for different channels
        self.exp_time = 0
        self.num_samples = {}

        # prepare zprocess events for communication between primary and secondary boards
        # primary board: boards/events = list of all secondary board names/events
        # secondary board: boards/events = list containing only primary board name/event
        if self.is_primary:
            self.events = [self.process_tree.event('%s_evt'%s, role='both') for s in self.boards]
        else:
            self.events = [self.process_tree.event('%s_evt' % self.device_name, role='both')]

    def sync_boards(self, id, payload=None):
        # synchronize multiple boards
        # give unique event id in steps of 2 for each type of event.
        # 1. primary board waits for events of all secondary boards and then sends event back.
        # 2. each secondary board sends event to primary board and waits for primary event.
        # primary collects dictionary {board_name:payload} for all boards and sends back to all boards.
        # this allows to share data among boards.
        # returns (timeout=True/False, dictionary of payloads or empty if None received)
        # this function acts like a "barrier" for all workers:
        # i.e. function exits only when all boards have entered it.
        timeout = False
        if self.is_primary:
            # 1. primary board: first wait then send
            payload = {} if payload is None else {self.device_name:payload}
            for i,event in enumerate(self.events):
                try:
                    t_start = get_ticks()
                    result = event.wait(id, timeout=1.0)
                    if result is not None: payload[self.boards[i]] = result
                    #print("run %i event %i board '%s' result '%s' (%.3fms wait time)" % (self.run_count, id, self.boards[i], str(result), (get_ticks() - t_start)*1e3))
                except zTimeoutError:
                    timeout = True
            for event in self.events:
                event.post(id + 1, data=None if len(payload) == 0 else payload)
            # return collected payload from boards
            result = payload
        else:
            # 2. secondary board: first send then wait
            self.events[0].post(id, data=payload)
            try:
                t_start = get_ticks()
                result = self.events[0].wait(id + 1, timeout=1.0)
                #print("run %i event %i board '%s' result '%s' (%.3fms wait time)" % (self.run_count, id, self.boards[0], str(result), (get_ticks() - t_start) * 1e3))
            except zTimeoutError:
                timeout = True
                result = None

        return (timeout, result)

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
                self.num_samples = {}
                update = True

                # load data tables for all output channels
                for connection, device in self.channels.items():
                    hardware_type = device.hardware_info[DEVICE_INFO_TYPE]
                    group = f[device.hardware_info[DEVICE_INFO_PATH]]
                    times = group[DEVICE_TIME][()]
                    static = False
                    if hardware_type == HARDWARE_TYPE_AO:
                        devices = [(device.name, DEVICE_DATA_AO % (device.name, device.hardware_info[DEVICE_INFO_ADDRESS]),device.parent_port)]
                        try:
                            self.num_samples['AO'] += len(times)
                        except KeyError:
                            self.num_samples['AO'] = len(times)
                    elif hardware_type == HARDWARE_TYPE_STATIC_AO:
                        devices = [(device.name, DEVICE_DATA_AO % (device.name, device.hardware_info[DEVICE_INFO_ADDRESS]),device.parent_port)]
                        try:
                            self.num_samples['AO (static)'] += 1
                        except KeyError:
                            self.num_samples['AO (static)'] = 1
                        static = True
                    elif (hardware_type == HARDWARE_TYPE_DO):
                        devices = [(device.name, DEVICE_DATA_DO % (device.hardware_info[DEVICE_INFO_BOARD], device.hardware_info[DEVICE_INFO_ADDRESS]), device.parent_port)]
                        try:
                            self.num_samples['DO'] += len(times)
                        except KeyError:
                            self.num_samples['DO'] = len(times)
                    elif (hardware_type == HARDWARE_TYPE_STATIC_DO):
                        devices = [(device.name, DEVICE_DATA_DO % (device.hardware_info[DEVICE_INFO_BOARD], device.hardware_info[DEVICE_INFO_ADDRESS]), device.parent_port)]
                        try:
                            self.num_samples['DO (static)'] += 1
                        except KeyError:
                            self.num_samples['DO (static)'] = 1
                        static = True
                    elif (hardware_type == HARDWARE_TYPE_TRG):
                        devices = [(device.name, DEVICE_DATA_DO % (device.hardware_info[DEVICE_INFO_BOARD], device.hardware_info[DEVICE_INFO_ADDRESS]), device.parent_port)]
                    elif hardware_type == HARDWARE_TYPE_DDS:
                        devices = [(channel.name, DEVICE_DATA_DDS % (device.name, device.hardware_info[DEVICE_INFO_ADDRESS], channel.parent_port), channel.parent_port) for channel in device.child_list.values()]
                        try:
                            self.num_samples['DDS'] += len(times)
                        except KeyError:
                            self.num_samples['DDS'] = len(times)
                    else:
                        print("warning: device %s unknown type %s (skip)" % (device.name, hardware_type))
                        continue
                    final = {}
                    for (name, dataset, port) in devices:
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
            (timeout, board_times) = self.sync_boards(id=EVENT_TRANSITION_TO_BUFFERED, payload=self.exp_time)
            if timeout:
                print("\ntimeout sync with all boards!\n")
                return None
            print('board times:', board_times)

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
            for name, samples in self.num_samples.items():
                tmp += ['%s: %i samples' % (name, samples)]
            print("%s done %s (ok)" % (self.device_name, ','.join(tmp)))

            # get status (error) of all boards
            (timeout, self.board_status) = self.sync_boards(EVENT_TRANSITION_TO_MANUAL, payload=error)
            if timeout:
                print("\ntimeout get status of all boards!\n")
                return None
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
    
