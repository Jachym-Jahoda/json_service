package main

file, err := os.Open("data.json")
if err != nil {
log.Println("Error opening json file:", err)
}
defer file.Close()

data, err := ioutil.ReadAll(file)
if err != nil {
log.Println("Error reading json data:", err)
}

var jsonfile []Data
err = json.Unmarshal(data, &jsonfile)
if err != nil {
log.Println("Error unmarshalling json data:", err)
}
dataTime := time.Now()
for _, data := range jsonfile {
digitalPorts, analogPorts := downloadPortsForDevice(device ,db)

if data.Type == "digital" {
for _, port := range digitalPorts {
dataTime, err = time.Parse("2006-1-2 15:4:5.000", data.Datetime)
if err != nil {
fmt.Println(color.Ize(color.Red, "ERR Problem parsing date time for digital data."+err.Error()))
continue
}
var digitalRecord database.DevicePortDigitalRecord
digitalRecord.DevicePortID = int(port.ID)
digitalRecord.DateTime = dataTime
digitalRecord.Data = int(data.Data)

db.Create(&digitalRecord)
devicesPortDigitalSync.Lock()
devicePortDigitalData[port.ID] = DevicePortDigitalData{
DateTime: dataTime,
Data:     int(data.Data),
}
devicesPortDigitalSync.Unlock()
}
} else if data.Type == "analog" {
for _, port := range analogPorts {
maxPositionsInAnalogData := 10
dataTime, err = time.Parse("2006-1-2 15:4:5", data.Datetime)
if err != nil {
fmt.Println(color.Ize(color.Red, "ERR Problem parsing date time for analog data."+err.Error()))
continue
}
if port.PortNumber <= maxPositionsInAnalogData {
var analogRecord database.DevicePortAnalogRecord
analogRecord.DevicePortID = int(port.ID)
analogRecord.DateTime = dataTime
analogRecord.Data = data.Data

db.Create(&analogRecord)
devicesPortAnalogSync.Lock()
devicePortAnalogData[port.ID] = DevicePortAnalogData{
DateTime: dataTime,
Data:     data.Data,
}
devicesPortAnalogSync.Unlock()
}
}
}
