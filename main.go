package main
import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)
// COVID Structure
type COVID struct {
	Positive   int    `json:"positive"`
	Tests      int    `json:"tests"`
	Date       string `json:"date"`
	Discharged int   ` json:"discharged"`
	Expired    int    `json:"expired"`
	Admitted   int    `json:"admitted"`
	Region     string `json:"region"`
}
type ServerResponse struct {
	Response []COVID `json:"response"`
}
type QueryFields struct {
	Region string `json:"region"`
	Date   string `json:"date"`
}
type QueryCommand struct {
	QueryFields QueryFields `json:"query"`
}
func main() {
	network := "tcp"
	addr := ":4040"
	csvFile, _ := os.Open("covid_final_data.csv")
	reader := csv.NewReader(bufio.NewReader(csvFile))
	var covid []COVID
	var provinceMap = make(map[string][]COVID)
	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}
		positive, err := strconv.Atoi(line[0])
		if err != nil {
			continue
		}
		tests, err := strconv.Atoi(line[1])
		if err != nil {
			continue
		}
		discharged, err := strconv.Atoi(line[3])
		if err != nil {
			continue
		}
		expired, err := strconv.Atoi(line[4])
		if err != nil {
			continue
		}
		admitted, err := strconv.Atoi(line[6])
		if err != nil {
			continue
		}
		c := COVID{
			Positive:   positive,
			Tests:      tests,
			Date:       line[2],
			Discharged: discharged,
			Expired:    expired,
			Region:     line[5],
			Admitted:   admitted,
		}
		covid = append(covid, c)
		provinceMap[line[5]] = append(provinceMap[line[5]], c)
	}

	/*var srvResp ServerResponse
	srvResp.Response = covid
	output, _ := json.Marshal(srvResp)
	fmt.Println(string(output))*/
	ln, err := net.Listen(network, addr)
	if err != nil {
		log.Fatal("failed to create listener:", err)
	}
	defer ln.Close()
	log.Println("Corona Virus Pakistan Dataset 2020 Service")
	log.Printf("Service started: (%s) %s\n", network, addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			if err := conn.Close(); err != nil {
				log.Println("failed to close listener", err)
			}
			continue
		}
		log.Println("Connected to", conn.RemoteAddr())
		go handleConnection(conn, covid, provinceMap)
	}
}
func handleConnection(conn net.Conn, dataset []COVID, pDataSet map[string][]COVID) {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Println("error closing connection:", err)
		}
	}()
	var query QueryCommand
	var srvResp ServerResponse
	for {
		cmdLine := make([]byte, 1024*4)
		n, err := conn.Read(cmdLine)
		if n == 0 || err != nil {
			log.Println("connection read error", err)
			return
		}
		err = json.Unmarshal(cmdLine[:n], &query); if err != nil {
			log.Println("error parsing command", err)
		}
		subset, err := filterData(query, dataset, pDataSet)
		if err != nil {
			log.Println("error filtering data", err)
		}
		srvResp.Response = subset
		output, err := json.MarshalIndent(srvResp, "", " ")
		if err != nil {
			log.Println("error marshaling json output", err)
		}
		_, err = conn.Write(output)
		if err != nil {
			log.Println("error writing output")
		}
	}
}
func filterData(query QueryCommand, data []COVID, pDataSet map[string][]COVID) (subset []COVID, err error) {
	if query.QueryFields.Region != "" {
		subset = pDataSet[query.QueryFields.Region]
		// for _, v := range data {
		// 	if strings.ToLower(v.Region) == strings.ToLower(query.QueryFields.Region){
		// 		subset = append(subset, v)
		// 	}
		// }
	} else if query.QueryFields.Date != "" {
		for _, v := range data {
			if strings.ToLower(v.Date) == strings.ToLower(query.QueryFields.Date){
				subset = append(subset, v)
			}
		}
	} else {
		log.Println("invalid query")
	}
	return
}