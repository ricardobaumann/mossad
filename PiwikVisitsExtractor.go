package main

import (
	"net/http"
	"io/ioutil"
	"fmt"
	"time"
	"regexp"
	"strings"
	"sort"
	"encoding/json"
	"os"
	"strconv"
)


type ActionDetail struct {
	Url string `json:"url"`
}

type Visit struct {
	ActionDetails []ActionDetail `json:"actionDetails"`
}

func main() {
	loadResults()
}

func loadResults() (map[string][]string) {
	piwikBaseUrl := "https://piwik-admin.up.welt.de/index.php?module=API&method=Live.getLastVisitsDetails&format=JSON&showColumns=actionDetails,deviceType" +
		"&idSite=1&period=day&date=today&expanded=1&filter_sort_column=lastActionTimestamp&filter_sort_order=desc" +
			"&token_auth="+piwikToken
	limitPerContentId, _ := strconv.Atoi(os.Getenv("limitPerContentId"))
	limit, _ := strconv.Atoi(os.Getenv("limitPerPage"))
	maxPages, _ := strconv.Atoi(os.Getenv("maxPages"))
	throughput, _ := strconv.Atoi("throughput")

	start := time.Now()

	reg, _ := regexp.Compile("[^0-9]+")
	pageResultsChannel := make(chan []Visit)

	controlChannel := make(chan bool, throughput)
	for i:=0; i < throughput; i++ {
		go putOnChannel(true, controlChannel)
	}

	for page := 0; page < maxPages; page++ {
		go loadPage(piwikBaseUrl,limit, page*limit, pageResultsChannel, controlChannel)
	}
	var relationsMap = make(map[string][]string)
	for page := 0; page < maxPages; page++ {
		visits := <- pageResultsChannel
		for _, visit := range visits {
			var contentIdList []string

			for _, detail := range visit.ActionDetails {
				parts := strings.Split(detail.Url,"/")
				contentId := reg.ReplaceAllString(parts[len(parts)-2],"")
				contentIdList = append(contentIdList,contentId)
			}

			combineKeys(contentIdList,&relationsMap)
		}
	}

	resultsToBeSaved := make(map[string][]string)
	for k, v := range relationsMap {
		resultsToBeSaved[k] = countAndOrder(v, limitPerContentId)
	}
	t := time.Now()
	elapsed := t.UnixNano() - start.UnixNano()
	println(elapsed)
	return resultsToBeSaved
}
func putOnChannel(value bool, channel chan bool) {
	channel <- value
}
func countAndOrder(v []string, limitPerContentId int) ([]string) {
	countMap := wordCount(v)
	countIndexMap := make(map[int][]string)
	var resultList []string
	var countList []int
	for k,v := range countMap {
		_, ok := countIndexMap[v]
		if !ok {
			countIndexMap[v] = []string{}
		}
		countIndexMap[v] = append(countIndexMap[v],k)
		countList = append(countList,v)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(countList)))
	for _, countValue := range countList {

		for _, internalId := range countIndexMap[countValue] {
			resultList = append(resultList,internalId)
		}
	}
	var length = len(resultList)
	var topResults = resultList
	if length > limitPerContentId {
		topResults = resultList[0:limitPerContentId]
	}

	return topResults
}
func combineKeys(contentIdList []string, relationsMap *map[string][]string) {
	for _, id := range contentIdList {
		if id != "" {
			_, ok := (*relationsMap)[id]
			if !ok {
				(*relationsMap)[id] = []string{}
			}
			newList := (*relationsMap)[id]
			for _, internalId := range contentIdList {
				if id != internalId && internalId != "" {
					newList = append(newList,internalId)
				}
			}
			(*relationsMap)[id] = newList
		}

	}
}

func loadPage(baseUrl string,limit int, offset int, pageResultsChannel chan []Visit, controlChannel chan bool) {
	<-controlChannel
	timeout := time.Duration(600 * time.Second)
	httpClient := http.Client{
		Timeout: timeout,
	}
	url := fmt.Sprintf("%s&filter_limit=%d&filter_offset=%d",baseUrl, limit, offset)
	resp, err := httpClient.Get(url)
	defer resp.Body.Close()
	if err == nil {
		bodyContent,err := ioutil.ReadAll(resp.Body)
		if err == nil {
			var visits []Visit
			json.Unmarshal(bodyContent, &visits)
			pageResultsChannel <- visits
		} else {
			println(string(err.Error()))
			pageResultsChannel <- []Visit{}
		}
	} else {
		println(string(err.Error()))
		pageResultsChannel <- []Visit{}
	}
	controlChannel <- true
}

func wordCount(words []string) map[string]int {
	wordFreq := make(map[string]int)
	for _, word := range words {
		_, ok := wordFreq[word]
		if ok == true {
			wordFreq[word] += 1
		} else {
			wordFreq[word] = 1
		}
	}
	return wordFreq
}

