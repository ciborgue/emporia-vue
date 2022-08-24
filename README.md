# emporia-vue
Python script to download Emporia VUE data to InfluxDB

# Installation
- Update configuration parameters in JSON file
- Add to your `crontab`

# Performance
Performance is limited by the Emporia API, if you observe 500s please
change window size from default 5 minutes to a bigger value and lower
load factor. This will increase transfer time though.
