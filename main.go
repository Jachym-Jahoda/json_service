package main

import (
	"encoding/json"
	"fmt"
	"github.com/TwiN/go-color"
	"github.com/hyperboloide/lk"
	"github.com/kardianos/service"
	"github.com/petrjahoda/database"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const version = "2022.3.1.27"
const serviceName = "Json Service"
const serviceDescription = "unwraps json data and saves it into database"
const downloadInSeconds = 10
const config = "user=postgres password=pj79.. dbname=system host=localhost port=5432 sslmode=disable application_name=json_service"

var (
	serviceIsRunning = false
	serviceSync      sync.Mutex
)

var (
	countValidDevices string
	countMux          sync.RWMutex
)

var (
	deviceTypeId      uint
	deviceTypeIdMutex sync.RWMutex
)

var (
	activeDevices  = map[uint]database.Device{}
	runningDevices = map[uint]database.Device{}
	devicesSync    sync.Mutex
)

var (
	devicePortDigitalData  = map[uint]DevicePortDigitalData{}
	devicesPortDigitalSync sync.RWMutex
)

var (
	devicePortAnalogData  = map[uint]DevicePortAnalogData{}
	devicesPortAnalogSync sync.RWMutex
)

type DevicePortDigitalData struct {
	DateTime time.Time
	Data     int
}

type DevicePortAnalogData struct {
	DateTime time.Time
	Data     float32
}

type Data struct {
	Type     string  `json:"type"`
	Port     uint    `json:"port"`
	Datetime string  `json:"datetime"`
	Data     float32 `json:"data"`
}

type program struct{}

func main() {
	fmt.Println(color.Ize(color.Green, "INF [MAIN] "+serviceName+" ["+version+"] starting..."))
	fmt.Println(color.Ize(color.Green, "INF [MAIN] © "+strconv.Itoa(time.Now().Year())+" Petr Jahoda"))
	serviceConfig := &service.Config{
		Name:        serviceName,
		DisplayName: serviceName,
		Description: serviceDescription,
	}
	prg := &program{}
	s, err := service.New(prg, serviceConfig)
	if err != nil {
		fmt.Println(color.Ize(color.Red, "ERR [MAIN] Cannot start: "+err.Error()))
	}
	err = s.Run()
	if err != nil {
		fmt.Println(color.Ize(color.Red, "ERR [MAIN] Cannot start: "+err.Error()))
	}
}

func (p *program) Start(service.Service) error {
	fmt.Println(color.Ize(color.Green, "INF [MAIN] "+serviceName+" ["+version+"] started"))
	go p.run()
	return nil
}

func (p *program) Stop(service.Service) error {
	fmt.Println(color.Ize(color.Green, "INF [MAIN] "+serviceName+" ["+version+"] stopped"))
	return nil
}

func (p *program) run() {
	db, _ := gorm.Open(postgres.Open(config), &gorm.Config{})
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	programIsActive := false
	checkDatabaseConnection()
	updateProgramVersion(db)
	readLatestPortData(db)
	readDeviceType(db)
	for {
		programIsActive = checkActivation(db)
		readActiveDevices(programIsActive, db)
		fmt.Println(color.Ize(color.Green, "INF [MAIN] License activated: "+strconv.FormatBool(programIsActive)))
		fmt.Println(color.Ize(color.Green, "INF [MAIN] "+serviceName+" ["+version+"] running"))
		fmt.Println(color.Ize(color.Green, "INF [MAIN] © "+strconv.Itoa(time.Now().Year())+" Petr Jahoda"))
		start := time.Now()
		devicesSync.Lock()
		fmt.Println(color.Ize(color.Green, "INF [MAIN] Active devices: "+strconv.Itoa(len(activeDevices))+", running devices: "+strconv.Itoa(len(runningDevices))))
		devices := activeDevices
		devicesSync.Unlock()
		for _, device := range devices {
			devicesSync.Lock()
			_, activeDeviceIsRunning := runningDevices[device.ID]
			devicesSync.Unlock()
			if !activeDeviceIsRunning {
				go runDevice(device, db)
			}
		}
		if time.Since(start) < (downloadInSeconds * time.Second) {
			sleepTime := downloadInSeconds*time.Second - time.Since(start)
			fmt.Println(color.Ize(color.Green, "INF [MAIN] Sleeping for "+sleepTime.String()))
			time.Sleep(sleepTime)
		}
	}
}

func readDeviceType(db *gorm.DB) {
	var databaseDeviceType database.DeviceType
	db.Where("name=?", "Json").Find(&databaseDeviceType)
	deviceTypeIdMutex.Lock()
	deviceTypeId = databaseDeviceType.ID
	deviceTypeIdMutex.Unlock()
}

func checkDatabaseConnection() {
	databaseConnected := false
	for !databaseConnected {
		db, err := gorm.Open(postgres.Open(config), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		sqlDB, _ := db.DB()
		if err != nil {
			fmt.Println(color.Ize(color.Red, "ERR [SYSTEM] Database not connected: "+err.Error()))
			time.Sleep(1 * time.Second)
		} else {
			var checkUser database.User
			db.Where("email = ?", "admin@admin.com").Find(&checkUser)
			if checkUser.ID == 0 {
				fmt.Println(color.Ize(color.Red, "ERR [SYSTEM] Database not initialized"))
				sqlDB.Close()
				time.Sleep(1 * time.Second)
			} else {
				sqlDB.Close()
				databaseConnected = true
			}
		}
	}
}

func readLatestPortData(db *gorm.DB) {
	fmt.Println(color.Ize(color.Green, "INF [MAIN] Reading latest port data"))
	timer := time.Now()
	var digitalRecords []database.DevicePortDigitalRecord
	db.Raw("select * from device_port_digital_records where id in (select distinct max(id) as id from device_port_digital_records group by device_port_id)").Find(&digitalRecords)
	var analogRecords []database.DevicePortAnalogRecord
	db.Raw("select * from device_port_analog_records where id in (select distinct max(id) as id from device_port_analog_records group by device_port_id)").Find(&analogRecords)
	devicesPortDigitalSync.Lock()
	for _, record := range digitalRecords {
		var data DevicePortDigitalData
		data.Data = record.Data
		data.DateTime = record.DateTime
		devicePortDigitalData[uint(record.DevicePortID)] = data
	}
	devicesPortDigitalSync.Unlock()
	devicesPortAnalogSync.Lock()
	for _, record := range analogRecords {
		var data DevicePortAnalogData
		data.Data = record.Data
		data.DateTime = record.DateTime
		devicePortAnalogData[uint(record.DevicePortID)] = data
	}
	devicesPortAnalogSync.Unlock()
	fmt.Println(color.Ize(color.Green, "INF [MAIN] Latest port data read in "+time.Since(timer).String()))
}

func downloadPortsForDevice(device database.Device, db *gorm.DB) ([]database.DevicePort, []database.DevicePort) {
	var digitalPorts []database.DevicePort
	db.Where("device_id = ?", device.ID).Where("device_port_type_id = ?", 1).Where("virtual = ?", false).Find(&digitalPorts)
	var analogPorts []database.DevicePort
	db.Where("device_id = ?", device.ID).Where("device_port_type_id = ?", 2).Where("virtual = ?", false).Find(&analogPorts)
	return digitalPorts, analogPorts
}

func updateProgramVersion(db *gorm.DB) {
	fmt.Println(color.Ize(color.Green, "INF [MAIN] Writing program version into settings"))
	timer := time.Now()
	var existingSettings database.Setting
	db.Where("name=?", serviceName).Find(&existingSettings)
	existingSettings.Name = serviceName
	existingSettings.Value = version
	db.Save(&existingSettings)
	fmt.Println(color.Ize(color.Green, "INF [MAIN] Program version written into settings in "+time.Since(timer).String()))
}

func readActiveDevices(programIsActive bool, db *gorm.DB) {
	fmt.Println(color.Ize(color.Green, "INF [MAIN] Reading active devices"))
	timer := time.Now()
	deviceTypeIdMutex.RLock()
	databaseDeviceTypeId := deviceTypeId
	deviceTypeIdMutex.RUnlock()
	var devices []database.Device
	if !programIsActive {
		db.Where("device_type_id=?", databaseDeviceTypeId).Where("activated = true").Limit(1).Find(&devices)
	} else {
		countMux.RLock()
		limitDevicesTo, err := strconv.Atoi(countValidDevices)
		countMux.RUnlock()
		if err != nil {
			db.Where("device_type_id=?", databaseDeviceTypeId).Where("activated = true").Limit(1).Find(&devices)
		}
		db.Where("device_type_id=?", databaseDeviceTypeId).Where("activated = true").Limit(limitDevicesTo).Find(&devices)
	}
	devicesSync.Lock()
	activeDevices = make(map[uint]database.Device, len(devices))
	for _, device := range devices {
		activeDevices[device.ID] = device
	}
	devicesSync.Unlock()
	fmt.Println(color.Ize(color.Green, "INF [MAIN] Active devices read in "+time.Since(timer).String()))
}

func checkActivation(db *gorm.DB) bool {
	var customerName database.Setting
	var softwareLicense database.Setting
	db.Where("name = ?", "company").Find(&customerName)
	db.Where("name = ?", serviceName).Find(&softwareLicense)
	const publicKeyBase32 = "ARIVIK3FHZ72ERWX6FQ6Z3SIGHPSMCDBRCONFKQRWSDIUMEEESQULEKQ7J7MZVFZMJDFO6B46237GOZETQ4M2NE32C3UUNOV5EUVE3OIV72F5LQRZ6DFMM6UJPELARG7RLJWKQRATUWD5YT46Q2TKQMPPGIA===="
	publicKey, err := lk.PublicKeyFromB32String(publicKeyBase32)
	if err != nil {
		return false
	}
	license, err := lk.LicenseFromB32String(softwareLicense.Note)
	if err != nil {
		return false
	}
	if ok, err := license.Verify(publicKey); err != nil {
		return false
	} else if !ok {
		return false
	}
	result := struct {
		Software string `json:"software"`
		Customer string `json:"customer"`
	}{}
	if err := json.Unmarshal(license.Data, &result); err != nil {
		return false
	}
	if len(strings.Split(result.Software, ":")) != 2 {
		return false
	}
	softwareName := strings.Split(result.Software, ":")[0]
	countMux.Lock()
	countValidDevices = strings.Split(result.Software, ":")[1]
	countMux.Unlock()
	if err != nil {
		return false
	}
	if result.Customer == customerName.Value && softwareName == softwareLicense.Name {
		fmt.Println(color.Ize(color.Green, "INF [MAIN] "+serviceName+": licence is valid for "+strings.Split(result.Software, ":")[1]+" devices"))
		return true
	}
	return false
}

func runDevice(device database.Device, db *gorm.DB) {
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
		digitalPorts, analogPorts := downloadPortsForDevice(device, db)
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
	}
}
