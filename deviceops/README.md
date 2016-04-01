# Iobeam telemetry spark app

Stateful series processing with filtering and triggers

### Filters

Filters work on individual series and outputs a new value on each sample. They can be attached to the
output from other filters but will then operate on the data from the previous batch.

### Triggers

Triggers is evaluated on each sample and outputs a trigger name or None. Triggers can be 
attached to derived series.

### Metrics

A metric keeps state about a series or device and outputs a periodical series with the state. 

## Examples

### Low battery level

A common use of the trigger system is to generate events when a certain metric passes a threshold. 
For example when the battery level of a device is below a defined level. To setup a trigger for when 
the series named "battery" is below 10 %, you setup a ThresholdTrigger:

Builder().
addTrigger("battery", ThresholdTrigger(10.0, "Battery below 10%", 15.0)).build()

where 10.0 is the threshold level and 15.0 is the trigger release level. This hysteresis 
means that the trigger will not create multiple events if the battery readings oscillate around the 
trigger level. 

### Monitor when CPU is above threshold and when it goes below another threshold

When monitoring a metric where you are interested both when it enters and leaves a problematic area, 
such as high CPU, you configure the threshold trigger

new DeviceMonitoringConfigurationBuilder().  
addTrigger("cpu", ThresholdTrigger(90.0,"CPU above 90%", 70.0, "CPU below 70%")).build()

This will create a trigger event when the cpu readings go above 90% and another event when the series 
go below 70 %

### High CPU load for extended period

To set a threshold trigger that detects when a series is above a threshold longer than a timeout, 

Builder().
addTrigger("Series name", ThresholdTimeoutTrigger(0.7, 0.5, Milliseconds(100), "Above threshold longer than 100 ms")).build

### Detect quickly changing series

Detecting quick changes in series can be done by connecting a threshold trigger to a derivative filter. 

new DeviceMonitoringConfigurationBuilder().  
addFilter("cpu", "cpu_derived", Filter.DeriveFilter).
addTrigger("cpu_derived", ThresholdTrigger(1.0,"CPU increase high", 0.0 , "CPU leveled out"))

As noise on a series can make the derivative very jumpy, a smoothing filter can be applied before
  
new DeviceMonitoringConfigurationBuilder().  
addFilter("noisy_series", "smoothed_series", new Ewma(0.1))).
addFilter("smoothed_series", "series_derived", new DeriveFilter).
addTrigger("series_derived", new ThresholdTrigger(1.0,"Series increase high", 0.0 , "Series leveled out"))
.build()
  
For irregular sampled series, EwmaIrregular can be used instead.  

### Change in frequency of events

TODO with leaky bucket style filter/metric

### Device offline

new DeviceMonitoringConfigurationBuilder()
.addDeviceTrigger(new DeviceTimeoutTrigger(Seconds(10), "device offline"))
.build