# internal pseudoclock device
# created April 2024 by Andi
# last change 23/4/2024 by Andi

# TODO: when needed implement split_connection, combine_channel_data, extract_channel_data and generate_code for your device.

from labscript import (
    IntermediateDevice,
    AnalogOut, StaticAnalogOut,
    DigitalOut, StaticDigitalOut, Trigger,
    DDS,
    LabscriptError,
    Pseudoclock,
    ClockLine,
    PseudoclockDevice,
    set_passed_properties,
    config,
)

import numpy as np
from time import perf_counter as get_ticks

# reduce number of log entries in logfile (labscript-suite/logs/BLACS.log)
import logging
log_level = [logging.CRITICAL, logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG, logging.NOTSET][2]

# for testing
#from user_devices.h5_file_parser import read_file, read_group

# connection strings
CON_SEP                 = '/'
CON_PS                  = 'ps'
CON_CL                  = 'cl'

# clockline name returned by split_connection for board name + type + address
NAME_CLOCKLINE          = '%s_%s_%s'
NAME_AO                 = 'ao'
NAME_DO                 = 'do'
NAME_STATIC_AO          = 'ao_static'
NAME_STATIC_DO          = 'do_static'
NAME_DDS                = 'dds'
NAME_VIRTUAL            = 'virtual'
# pseudoclock, clockline and device name format strings for given clockline name
# NAME_DEV is displayed in runviwer_parser to user for the clockline. others are not visible to user.
NAME_PS                 = '%s_ps'
NAME_CL                 = '%s_cl'
NAME_DEV                = '%s_clock'

# virtual IM device. we use same connection format as digital channels.
VIRTUAL_ADDR            = '0'
VIRTUAL_CON             = VIRTUAL_ADDR + CON_SEP + '%i'

# hardware type strings
HARDWARE_TYPE_AO        = 'ao'
HARDWARE_TYPE_DO        = 'do'
HARDWARE_TYPE_STATIC_AO = 'aos'
HARDWARE_TYPE_STATIC_DO = 'dos'
HARDWARE_TYPE_TRG       = 'trg'
HARDWARE_TYPE_DDS       = 'dds'

# hd5 file name format for each IM device
DEVICE_SEP              = '/'
DEVICE_DEVICES          = 'devices'
DEVICE_TIME             = 'time'
DEVICE_DATA_AO          = 'data_ao_%s_%x'       # name + address
DEVICE_DATA_DO          = 'data_do_%s_%x'       # board name + address
DEVICE_DATA_DDS         = 'data_dds_%s_%x_%s'   # name + address + sub-channel name

# hardware info entry in connection table property
DEVICE_HARDWARE_INFO    = 'hardware_info'
DEVICE_INFO_PATH        = 'path'
DEVICE_INFO_ADDRESS     = 'address'
DEVICE_INFO_CHANNEL     = 'channel'
DEVICE_INFO_TYPE        = 'hardware_type'
DEVICE_INFO_BOARD       = 'parent_device'

class _iPCdev(Pseudoclock):
    def add_device(self, device):
        if isinstance(device, ClockLine):
            # only allow one child
            if self.child_devices:
                raise LabscriptError('The pseudoclock %s only supports 1 clockline, which is automatically created. Please use the clockline located at %s.clockline'%(self.parent_device.name, self.parent_device.name))
            Pseudoclock.add_device(self, device)
        else:
            raise LabscriptError('You have connected %s to %s (the pseudoclock of %s), but %s only supports children that are ClockLines. Please connect your device to %s.clockline instead.'%(device.name, self.name, self.parent_device.name, self.name, self.parent_device.name))

class iPCdev_device(IntermediateDevice):
    allowed_children = [DigitalOut, StaticDigitalOut, AnalogOut, StaticAnalogOut, DDS, Trigger]

    description = 'iPCdev intermediate device'

    def __init__(self, name, parent_device, board_name, clockline_name):
        IntermediateDevice.__init__(self, name, parent_device)
        # save parent board name (parent_device is the clockine, not the board)
        self.board_name = board_name
        # set connection to clockline_name of split_connection and get_device.
        # this might be used by blacs_tabs or worker.
        self.connection = clockline_name
        # hardware type identifies the type of connected hardware. we allow only one type per IM device.
        self.hardware_type  = None

    def add_device(self, device):
        # get hardware type
        if isinstance(device, Trigger):
            #print(self.name, 'add_device trigger', device.name)
            hardware_type = HARDWARE_TYPE_TRG
            # Trigger is automcatically created by labscript for secondary board(s)
            # we have to do the same as in iPCdev.add_device
            # 1. get clockline and hardware names. this uses always IPCdev implementation
            clockline_name, device.hardware_info = iPCdev.split_connection(self, device)
            # 2. save original parent_device name into hardware_info in case clockline is not of the same device.
            #    blacs_tabs uses this to display board devices only.
            device.hardware_info.update({DEVICE_INFO_BOARD:self.board_name})
            # 3. find or create intermediate device: not needed since this has already clockline/parent device.
            # 4. save device hardware information into connection table
            device.set_property(DEVICE_HARDWARE_INFO, device.hardware_info, location='connection_table_properties')
            # 5. add channel to intermediate device. done below.
        elif isinstance(device, (AnalogOut)):
            hardware_type = HARDWARE_TYPE_AO
        elif isinstance(device, (StaticAnalogOut)):
            hardware_type = HARDWARE_TYPE_STATIC_AO
        elif isinstance(device, (DigitalOut)):
            hardware_type = HARDWARE_TYPE_DO
        elif isinstance(device, (StaticDigitalOut)):
            hardware_type = HARDWARE_TYPE_STATIC_DO
        elif isinstance(device, DDS):
            hardware_type = HARDWARE_TYPE_DDS
        else:
            raise LabscriptError("device %s type %s added to device %s which is not allowed!" % (device.name, type(device), self.name))

        # check and set hardware type of IM device
        if self.hardware_type is None:
            self.hardware_type = hardware_type
            # save hardware type and board into connection table.
            # this allows blacs_tab and worker to determine the board and type of clockline.
            hardware_info = {DEVICE_INFO_TYPE: self.hardware_type, DEVICE_INFO_BOARD: self.board_name}
            self.set_property(DEVICE_HARDWARE_INFO, hardware_info, location='connection_table_properties')
        elif self.hardware_type != hardware_type:
            raise LabscriptError("device %s type %s added to intermediate device %s but already a different type %s existing!\nadd only same type to intermediate devices." % (device.name, hardware_type, self.name, self.hardware_type))

        # check if device was added directly (should not happen)
        if not hasattr(device, 'hardware_info'):
            raise LabscriptError("device %s type %s added directly to intermediate device %s!\nadd only to iPCdev PseudoClockDevice and not directly to intermediate device!" % (device.name, HARDWARE_TYPE_AO, self.name, self.hardware_type))

        # add channel to intermediate device
        IntermediateDevice.add_device(self, device)

class iPCdev(PseudoclockDevice):

    description         = 'internal pseudoclock device'
    clock_limit         = 100e6 # large enough such that the limit comes from the devices
    clock_resolution    = 1e-9
    trigger_delay       = 0
    wait_delay          = 0
    allowed_children    = [_iPCdev]
    max_instructions    = None

    # default data types for combine_channel_data
    AO_dtype            = np.float64
    DO_type             = np.uint32

    def __init__(self,
                 name,
                 parent_device = None,
                 AO_rate=1e6,
                 DO_rate=1e6,
                 worker_args = {},
                 BLACS_connection='internal pseudoclock device v1.0 by Andi',
                 ):

        self.name               = name
        self.primary            = parent_device    # None for primary device, otherwise primary device
        self.AO_rate            = AO_rate
        self.DO_rate            = DO_rate
        self.BLACS_connection   = BLACS_connection
        self.secondary          = []             # for primary device list of secondary devices, otherwise empty
        self.devices            = []             # for primary device list of all IM devices, otherwise empty

        # init device class
        if self.primary is None:
            print("iPCdev init primary", name)
            PseudoclockDevice.__init__(self, name, None, None)

            # for first board create virtual trigger IM device
            self.virtual_device = self.get_device(NAME_CLOCKLINE % (self.name, NAME_VIRTUAL, VIRTUAL_ADDR), True)
        else:
            # this needs the virtal trigger digital channel from the primary device as trigger_device\
            index = len(self.primary.secondary)
            print("iPCdev init sec %i"%index, name, "primary", self.primary.name, 'virtual IM', self.primary.virtual_device.name)
            PseudoclockDevice.__init__(self, name, trigger_device = self.primary.virtual_device, trigger_connection = (VIRTUAL_CON % index))

            # add this device to list of secondary boards of primary board
            self.primary.add_device(self)

        # save worker_args into connection_table
        self.set_property('worker_args', worker_args , location='connection_table_properties')

        # save module path. this allows to load runviewer_parser to load module and device class.
        self.set_property('derived_module', self.__module__, location='connection_table_properties')

    def add_device(self, device):
        if isinstance(device, Pseudoclock):
            # pseudoclock connected
            PseudoclockDevice.add_device(self, device)
        elif isinstance(device, iPCdev):
            # add secondary board
            self.secondary.append(device)
        else:
            # output channel connected: connect to proper IM device
            # 1. get clockline and hardware names. this might be overwritten in derived class
            # raises LabscriptError when connection is invalid
            clockline_name, device.hardware_info = self.split_connection(device)
            # 2. save original parent_device name into hardware_info in case clockline is not of the same device
            #    blacs_tabs uses this to get board devices only for diaplaying and to give to worker
            device.hardware_info.update({DEVICE_INFO_BOARD:self.name})
            # 3. find or create intermediate device
            device.parent_device = self.get_device(clockline_name, True)
            # 4. save device hardware information into connection table
            device.set_property(DEVICE_HARDWARE_INFO, device.hardware_info, location='connection_table_properties')
            # 5. add channel to intermediate device
            device.parent_device.add_device(device)

    def get_device(self, clockline_name, allow_create_new):
        """
        returns intermediate (IM) device for given clockline_name.
        either creates new pseudoclock + clockline + IM device or returns existing IM device.
        searchs IM device names in primary device list.
        if allow_create_new = False the device must exist, otherwise returns None
        if allow_create_new = True creates and returns new device if does not exists.
        notes:
        in default implementation the name is related to iPCdev device, so the IM device belongs to this board.
        however, get_device can be called from a derived class with arbitrary name independent of iPCdev device.
        this way IM device can be 'shared' between boards.
        depending on the implemenation of blacs_tabs the channels can be displayed with the board
        with which they are created or with the board having created the IM device.
        see NI_DAQmx implementation where this can be configured.
        """
        # check address and channel
        if clockline_name is None: return None

        # create name from device name and address
        _clockline_name = clockline_name.replace(CON_SEP,'_').replace(':','').replace('=','')
        name_ps  = NAME_PS  % (_clockline_name)
        name_cl  = NAME_CL  % (_clockline_name)
        name_dev = NAME_DEV % (_clockline_name)

        # search IM device with matching pseudoclock name
        devices = self.devices if self.primary is None else self.primary.devices
        for device in devices:
            if device.name == name_dev:
                # found: return IM device
                #print('IM device', device.name)
                return device

        # return None if not existing and we should not create a new one.
        if not allow_create_new: return None

        # not found: create new pseudoclock, clockline and intermediate device
        ps = _iPCdev(
            name                = name_ps,
            pseudoclock_device  = self,
            connection          = CON_PS,
        )
        cl = ClockLine(
            name                = name_cl,
            pseudoclock         = ps,
            connection          = CON_CL,
        )
        device = iPCdev_device(
            name                = name_dev,
            parent_device       = cl,
            board_name          = self.name,
            clockline_name      = clockline_name,
        )
        # add device to primary list of devices
        devices.append(device)
        # return new IM device
        return device

    ###############################################################
    # following functions might be overwritten in a derived class #
    ###############################################################

    def split_connection(self, channel):
        """
        TODO: overwrite in derived class if you need your own implementation.
        returns [clockline_name, hardware_info] for given output channel.
        hardware_info is saved into channel. and is given as channel.properties[HARDWARE_INFO],
                      also to device_tab and runviewer_parser.
        channel      = channel like AnalogOut, DigitalOut, DDS etc. given to add_device.
        raises LabscriptError on error.
        implementation details here:
        - channel.connection = "clockline/address/channel" given as string/integer/integer
          where address and channel can be prefixed with '0x' to indicate hex integer.
        - clockline can be omitted. then returns clockline_name = address string.
        - uses isinstance to determine type of device.
        - AnalogOutput: needs address only
        - DigitalOutput: needs address/channel
        - DDS: needs address only
        - returned clockline_name = clockline part or address if no clockline given
        - returned hardware_info = dict containing DEVICE_INFO_.. entries
          address = channel address integer
          channel = channel number for digital output or None for analog output
          hardware_type = HARDWARE_TYPE_ string of device
        - board_name is used only for error message
        - device_names is not used here.
        """
        clockline_name = None
        hardware_info  = {}
        connection = channel.connection
        if connection[0] == CON_SEP: connection = connection[1:] # remove initial '/'
        split = connection.split(CON_SEP)
        if isinstance(channel, Trigger):
            # Trigger device: connection = VIRTUAL_CON (same format as for digital channels)
            # note: for Trigger device isinstande (channel, DigitalOut) gives True!
            hardware_info[DEVICE_INFO_TYPE] = HARDWARE_TYPE_TRG
            try:
                if len(split) == 2: # address/channel
                    clockline_name                     = split[0]
                    hardware_info[DEVICE_INFO_ADDRESS] = int(split[0], 0)
                    hardware_info[DEVICE_INFO_CHANNEL] = int(split[1], 0)
            except ValueError:
                clockline_name = None
            if clockline_name is None:
                raise LabscriptError("trigger device '%s' connection '%s' (board '%s') invalid!\ngive '[clockline/]address/channel' as decimal or hex (with prefix 0x) integer." % (channel.name, channel.connection, self.name))
        elif isinstance(channel, (AnalogOut, StaticAnalogOut)):
            static = isinstance(channel, StaticAnalogOut)
            hardware_info[DEVICE_INFO_TYPE] = HARDWARE_TYPE_STATIC_AO if static else HARDWARE_TYPE_AO
            try:
                if len(split) == 1: # address only
                    clockline_name                     = NAME_CLOCKLINE % (self.name, NAME_AO, split[0])
                    hardware_info[DEVICE_INFO_ADDRESS] = int(split[0], 0)
                    hardware_info[DEVICE_INFO_CHANNEL] = None
                elif len(split) == 2: # clockline/address
                    clockline_name                     = NAME_CLOCKLINE % (self.name, NAME_AO, split[0])
                    hardware_info[DEVICE_INFO_ADDRESS] = int(split[1], 0)
                    hardware_info[DEVICE_INFO_CHANNEL] = None
            except ValueError:
                clockline_name = None
            if clockline_name is None:
                raise LabscriptError("AO device '%s' connection '%s' (board '%s') invalid!\ngive '[clockline/]address' as decimal or hex (with prefix 0x) integer." % (channel.name, channel.connection, board_name))
        elif isinstance(channel, (DigitalOut, StaticDigitalOut)):
            static = isinstance(channel, StaticDigitalOut)
            hardware_info[DEVICE_INFO_TYPE] = HARDWARE_TYPE_STATIC_DO if static else HARDWARE_TYPE_DO
            try:
                if len(split) == 2: # address/channel
                    clockline_name                     = NAME_CLOCKLINE % (self.name, NAME_DO, split[0])
                    hardware_info[DEVICE_INFO_ADDRESS] = int(split[0], 0)
                    hardware_info[DEVICE_INFO_CHANNEL] = int(split[1], 0)
                elif len(split) == 3: # clockline/address/channel
                    clockline_name                     = NAME_CLOCKLINE % (self.name, NAME_DO, split[0])
                    hardware_info[DEVICE_INFO_ADDRESS] = int(split[1], 0)
                    hardware_info[DEVICE_INFO_CHANNEL] = int(split[2], 0)
            except ValueError:
                clockline_name = None
            if clockline_name is None:
                raise LabscriptError("DO device '%s' connection '%s' (board '%s') invalid!\ngive '[clockline/]address/channel' as decimal or hex (with prefix 0x) integer." % (channel.name, channel.connection, self.name))
        elif isinstance(channel, (DDS)):
            hardware_info[DEVICE_INFO_TYPE] = HARDWARE_TYPE_DDS
            try:
                if len(split) == 1:  # address
                    clockline_name                     = NAME_CLOCKLINE % (self.name, NAME_DDS, split[0])
                    hardware_info[DEVICE_INFO_ADDRESS] = int(split[0], 0)
                    hardware_info[DEVICE_INFO_CHANNEL] = None
                elif len(split) == 2:  # clockline/address
                    clockline_name                     = NAME_CLOCKLINE % (self.name, NAME_DDS, split[0])
                    hardware_info[DEVICE_INFO_ADDRESS] = int(split[1], 0)
                    hardware_info[DEVICE_INFO_CHANNEL] = None
            except ValueError:
                clockline_name = None
            if clockline_name is None:
                raise LabscriptError("DDS device '%s' connection '%s' (board '%s') invalid!\ngive '[clockline/]address/channel' as decimal or hex (with prefix 0x) integer." % (channel.name, channel.connection, self.name))
        else:
            raise LabscriptError('You have connected %s (class %s) to %s, but does not support children with that class.'%(channel.name, channel.__class__, self.name))

        # note: hardware_info[DEVICE_INFO_PATH] is set later in generate_code.
        #       when split_connection is called channel.parent_device is the iPCdev board.
        #       the proper parent intermediate device is generated only after this returns.

        return clockline_name, hardware_info

    @staticmethod
    def combine_channel_data(hardware_info, channel_data, combined_channel_data):
        """
        TODO: overwrite in derived class if you need your own implementation.
        returns channel_data added to combined_channel_data for the given channel.
        hardware_info         = arbitrary device information to determine how data should be combined
        channel_data          = numpy array of raw data of channel. cannot be None.
        combined_channel_data = numpy array of combined data of all channels with same address.
                                can be None or np.empty for first device to be combined.
                                if not None returns the same data type,
                                otherwise uses default data types.: AO_dtype or DO_dtype.
        extract_channel_bits is the inverse function of this function.
        on error returns None
        implementation-details:
        - channel_info = dict with DEVICE_INFO_.. entries
        - address must be integer address of channel
        - if hardware_type == HARDWARE_TYPE_AO: analog output
          channel is None (but not checked)
          combined_channel_data must be empty or None since each analog output data is saved individually.
          returns channel_data
        - if hardware_type == HARDWARE_TYPE_AO: digital output
          channel must be an integer and gives the number of bits shifted left
          returns combined_channel_data | (channel_data << channel)
        """
        address       = hardware_info[DEVICE_INFO_ADDRESS]
        channel       = hardware_info[DEVICE_INFO_CHANNEL]
        hardware_type = hardware_info[DEVICE_INFO_TYPE]
        data = None
        if address is not None:
            if (hardware_type == HARDWARE_TYPE_AO)        or \
               (hardware_type == HARDWARE_TYPE_STATIC_AO) or \
               (hardware_type == HARDWARE_TYPE_DDS      ) :
                # analog out or DDS channel:
                # only one channel per address is allowed, i.e. give None or np.empty() as combined_channel_data
                if combined_channel_data is None:
                    data = channel_data.astype(iPCdev.AO_dtype)
                elif (len(combined_channel_data) == 0):
                    data = channel_data.astype(combined_channel_data.dtype)
            elif (hardware_type == HARDWARE_TYPE_DO       ) or \
                 (hardware_type == HARDWARE_TYPE_STATIC_DO) or \
                 (hardware_type == HARDWARE_TYPE_TRG      ) :
                # digital out: several channels per address combine data bits
                #              allow to give None or np.empty() for the first device
                if channel is not None and channel >= 0:
                    if combined_channel_data is None:
                        data = ((channel_data.astype(iPCdev.DO_type) & 1) << channel)
                    elif (len(combined_channel_data) == 0):
                        data = ((channel_data.astype(combined_channel_data.dtype) & 1) << channel)
                    else:
                        data = combined_channel_data | ((channel_data.astype(combined_channel_data.dtype) & 1) << channel)
        # return combined data or None on error
        return data

    @staticmethod
    def extract_channel_data(hardware_info, combined_channel_data):
        """
        TODO: overwrite in derived class if you need your own implementation.
        returns channel data from combined_channel_data for the given device.
        returns None on error.
        inverse function to combine_channel_aata. for description see there.
        """
        address       = hardware_info[DEVICE_INFO_ADDRESS]
        channel       = hardware_info[DEVICE_INFO_CHANNEL]
        hardware_type = hardware_info[DEVICE_INFO_TYPE]
        channel_data = None
        if (hardware_type == HARDWARE_TYPE_AO) or (hardware_type == HARDWARE_TYPE_STATIC_AO) or (hardware_type == HARDWARE_TYPE_DDS):
            channel_data = combined_channel_data
        elif (hardware_type == HARDWARE_TYPE_DO) or (hardware_type == HARDWARE_TYPE_STATIC_DO) or (hardware_type == HARDWARE_TYPE_TRG):
            if channel is not None and channel >= 0:
                channel_data = ((combined_channel_data >> channel) & 1).astype(bool)
        # return extracted channel data or None on error
        return channel_data

    def generate_code(self, hdf5_file):
        """
        TODO: overwrite in derived class if needed.
        save all times and data of all channels into hd5 file.
        this is called automatically also for secondary PseudoClockDevices.
        """
        print("%s generate_code ..." % self.name)
        t_start = get_ticks()

        if False:
            # test: look at user-provided instructions
            # TODO: for table-mode devices we might need to insert trigger commands...
            #       add a function which can be overwritten in derived class.
            for pseudoclock in self.child_devices:
                for clockline in pseudoclock.child_devices: # there should be only one
                    for IM in clockline.child_devices:
                        for dev in IM.child_devices:
                            if len(dev.instructions) > 0:
                                print(IM.name, dev.name, dev.instructions)

        PseudoclockDevice.generate_code(self, hdf5_file)
        group = hdf5_file[DEVICE_DEVICES].create_group(self.name)

        for pseudoclock in self.child_devices:
            for clockline in pseudoclock.child_devices: # there should be only one
                times = pseudoclock.times[clockline]
                for IM in clockline.child_devices:
                    # create IM device sub-group and save time
                    g_IM = group.create_group(IM.name)
                    g_IM.create_dataset(DEVICE_TIME, compression=config.compression, data=times)
                    # device path
                    path = DEVICE_DEVICES + DEVICE_SEP + self.name + DEVICE_SEP + IM.name
                    if IM.hardware_type is None:
                        # IM device created but has no channels?
                        print('warning: skip device %s without channels.' % (IM.name))
                    else:
                        if (IM.hardware_type == HARDWARE_TYPE_AO       ) or \
                           (IM.hardware_type == HARDWARE_TYPE_STATIC_AO):
                            # save data for each individual analog channel
                            for dev in IM.child_devices:
                                #print('AO', dev.name, 'address', dev.hardware_info[DEVICE_INFO_ADDRESS])
                                data = type(self).combine_channel_data(dev.hardware_info, dev.raw_output, None)
                                dataset = DEVICE_DATA_AO % (dev.name, dev.hardware_info[DEVICE_INFO_ADDRESS])
                                g_IM.create_dataset(dataset, compression=config.compression, data=data)
                                # save device path into device properties
                                dev.hardware_info[DEVICE_INFO_PATH] = path
                        elif (IM.hardware_type == HARDWARE_TYPE_DO       ) or \
                             (IM.hardware_type == HARDWARE_TYPE_STATIC_DO) or \
                             (IM.hardware_type == HARDWARE_TYPE_TRG      ):
                            # save data for digital channels and triggers for same board and address
                            # we could save each dataset for each device individually,
                            # but typically digital channels are grouped and share the same address = port number
                            # we assume that for each board the address is unique, but different boards might use the same addresses.
                            # therefore, we save dataset for each board and address.
                            # as long as the clocklines are not shared between boards, there will be anyway just one board in the list.
                            boards = set([dev.hardware_info[DEVICE_INFO_BOARD] for dev in IM.child_devices])
                            for board in boards:
                                # for all channels of the same board and address combine all bits into one data word
                                addresses = set([dev.hardware_info[DEVICE_INFO_ADDRESS] for dev in IM.child_devices if dev.hardware_info[DEVICE_INFO_BOARD] == board])
                                for address in addresses:
                                    data = None
                                    for dev in IM.child_devices:
                                        if (dev.hardware_info[DEVICE_INFO_BOARD] == board) and (dev.hardware_info[DEVICE_INFO_ADDRESS] == address):
                                            data = type(self).combine_channel_data(dev.hardware_info, dev.raw_output, data)
                                            #print('DO', dev.name, 'address', dev.hardware_info[DEVICE_INFO_ADDRESS], 'channel', dev.hardware_info[DEVICE_INFO_ADDRESS],'data', dev.raw_output, 'combined', data)
                                            # save device path into device properties
                                            dev.hardware_info[DEVICE_INFO_PATH] = path
                                    #print(board, 'DO address', address, 'data:', data)
                                    dataset = DEVICE_DATA_DO % (board, address)
                                    g_IM.create_dataset(dataset, compression=config.compression, data=data)
                        elif IM.hardware_type == HARDWARE_TYPE_DDS:
                            # save data for 3 analog channels: frequency, amplitude and phase
                            for dev in IM.child_devices:
                                #print('DDS', dev.name, 'address', dev.hardware_info[DEVICE_INFO_ADDRESS])
                                for subdev in dev.child_devices:
                                    #print('DDS', subdev.name)
                                    data = type(self).combine_channel_data(dev.hardware_info, subdev.raw_output, None)
                                    dataset = DEVICE_DATA_DDS % (dev.name, dev.hardware_info[DEVICE_INFO_ADDRESS], subdev.connection)
                                    g_IM.create_dataset(dataset, compression=config.compression, data=data)
                                # save device path into device properties
                                dev.hardware_info[DEVICE_INFO_PATH] = path
                        else:
                            print('warning: skip device %s hardware type %s' % (IM.name, IM.hardware_type))

        # this is needed
        self.set_property('stop_time', self.stop_time, location='device_properties')

        # note: when generate_code is not existing in intermediate device,
        #       then generate_code of secondary boards is called, otherwise not!
        #       for FPGA_board this is still not working, so seems other factors to matter as well?
        #for sec in self.secondary:
        #    print('%s call generate code for %s' % (self.name, sec.name))
        #    sec.generate_code(hdf5_file)

        # save if primary board and list of secondary boards names, or name of primary board.
        # the names identify the worker processes used for interprocess communication.
        if self.primary is None:
            self.set_property('is_primary', True, location='connection_table_properties', overwrite=False)
            self.set_property('boards', [s.name for s in self.secondary], location='connection_table_properties', overwrite=False)
        else:
            self.set_property('is_primary', False, location='connection_table_properties', overwrite=False)
            self.set_property('boards', [self.primary.name], location='connection_table_properties', overwrite=False)

        print("%s generate_code done (%.3f ms)" % (self.name, (get_ticks() - t_start) * 1000))
