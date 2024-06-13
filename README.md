# labscript_iPCdev
labscript-suite internal pseudoclock device

This is a generic labscript device which incorporates the basic functionality for devices with internal pseudoclock. This serves as a base class which implements the labscript functionality and allows to separate this from the hardware-related code which should be placed into derived classes where specific code is needed.

At the moment this is used by [NI_DAQmx_iPCdev](https://github.com/INO-quantum/labscript_NI_DAQmx_iPCdev) and [QRF](https://github.com/INO-quantum/labscript_QRF). In the near future I plan to use this also to simplfy the labscript driver for the [FPGA based experiment control system](https://github.com/INO-quantum/FPGA-SoC-experiment-control/tree/main/labscript-suite).

Please copy the `iPCdev` folder into your `user_devices` folder.

[Here](https://github.com/INO-quantum/labscript_iPCdev/tree/main/example_experiment) you find an example `connection_table` and an example experiment script.
