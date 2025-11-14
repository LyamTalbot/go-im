package main

//This will be the subscriber program
import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/valkey-io/valkey-go"
)

// message struct so I can marshall and unmarshall messages and have a little bit more information?
// will make it easier to construct and deconstruct/parse messages
type Message_t struct {
	Username string
	Message  string
	Time     time.Time
}

var messages = make([]string, 0)

// tview
var app = tview.NewApplication()
var flex = tview.NewFlex()
var inputBox = tview.NewInputField().SetLabel("Enter a Message: ").SetFieldTextColor(tcell.ColorGreen)
var messagesBox = tview.NewTextView()

// Persistence Stuff
var filePath = "./stream_id_records.csv"
var latestStreamIDs map[string]string

// chatDataStores
// var messageWindows = make(map[string]tview.TextView)
var messageHistories = make(map[string]string)

// var messageHistories = make(map[string]strings.Builder)

func main() {

	latestStreamIDs = buildStreamIDMap(filePath)
	messagesBox.SetBackgroundColor(tcell.ColorBlack)
	reader := bufio.NewScanner(os.Stdin)
	fmt.Print("Please enter your username: ")
	reader.Scan()
	username := reader.Text()
	fmt.Println(username)
	fmt.Print("Please enter your password: ")
	reader.Scan()
	password := reader.Text()

	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"192.168.50.238:6379"}, Username: username, Password: password, DisableCache: true})
	if err != nil {
		println("Client error")
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()

	flex.SetDirection(tview.FlexRow).
		AddItem(messagesBox, 0, 6, true).
		AddItem(tview.NewFlex().
			AddItem(inputBox, 0, 6, false), 0, 1, false).SetBorder(true)
	inputBox.SetDoneFunc(func(key tcell.Key) {
		message := inputBox.GetText()
		err := client.Do(ctx, client.B().Xadd().Key("chat").Id("*").FieldValue().FieldValue("writer", message).Build()).Error()
		if err != nil {
			panic(err.Error())
		}
		inputBox.SetText("")
	})

	//spin off message receving into it's own go routine
	//this way we will still recieve messages
	rebuildMessagesList(ctx, client, app)
	go receiveMessages(ctx, client, app)
	if err := app.SetRoot(flex, true).EnableMouse(true).Run(); err != nil {
		panic(err)
	}
	//Set key val NX

	//For subscribing to a channel we could just run this inside it's own go routine and have it update the UI.
	//Based on this we would just subscribe to every channel we are apart of.
	//If you wanted to think about it in terms of Discord servers, each server would be a channel
	//I think other thing that needs to be done would be to subscribe to our "own" channel, any messages that are sent to us from other
	//users should be sent to that user's channel.
	//once they're recieved we handle them, update the correct parts of the UI, store them seperately based on where they're from, etc.
}

func rebuildMessagesList(ctx context.Context, client valkey.Client, app *tview.Application) {
	var stringBuilder strings.Builder
	var message []valkey.XRangeEntry
	var err error
	messagesBox.SetTextColor(tcell.ColorLightGreen)
	message, err = client.Do(ctx, client.B().Xrevrange().Key("chat").End("+").Start("-").Count(100).Build()).AsXRange()
	if err != nil {
		panic(err)
	}
	slices.Reverse(message)
	for _, entry := range message {
		stringBuilder.WriteString(entry.FieldValues["writer"] + "\n")
		messages = append(messages, entry.FieldValues["writer"])
		messagesBox.SetText(stringBuilder.String())
		messagesBox.ScrollToEnd()
	}
}

func receiveMessages(ctx context.Context, client valkey.Client, app *tview.Application) {
	var message map[string][]valkey.XRangeEntry
	var err error
	for {
		id := latestStreamIDs["chat"]
		if id == "$" {
			message, err = client.Do(ctx, client.B().Xread().Block(0).Streams().Key("chat").Id("$").Build()).AsXRead()
			if err != nil {
				panic(err)
			}
			// parsedMessage = message["chat"][0].FieldValues["writer"]
			id = message["chat"][0].ID
		} else {
			message, err = client.Do(ctx, client.B().Xread().Block(0).Streams().Key("chat").Id(id).Build()).AsXRead()
			if err != nil {
				panic(err)
			}
			// parsedMessage = message["chat"][0].FieldValues["writer"]
			id = message["chat"][0].ID
		}
		// message, err := client.Do(ctx, client.B().Xread().Block(0).Streams().Key("chat").Id("$").Build()).AsXRead()
		if err != nil {
			fmt.Println(err.Error())
		}
		parsedMessage := message["chat"][0].FieldValues["writer"]
		messages = append(messages, parsedMessage)
		// messagesList.AddItem(parsedMessage, "", rune(0), nil)
		messagesBox.SetText(messagesBox.GetText(true) + "\n" + parsedMessage)
		latestStreamIDs["chat"] = id
		messagesBox.ScrollToEnd()
		saveStreamIDs(latestStreamIDs)
		app.Draw()
	}
}

func readFromStream(stream string, ctx context.Context, client valkey.Client, app *tview.Application) {
	id := ""
	var message map[string][]valkey.XRangeEntry
	var err error
	for {
		if id == "" {
			message, err = client.Do(ctx, client.B().Xread().Block(0).Streams().Key(stream).Id("$").Build()).AsXRead()
			if err != nil {
				panic(err)
			}
			id = message["chat"][0].ID
		} else {
			message, err = client.Do(ctx, client.B().Xread().Block(0).Streams().Key(stream).Id(id).Build()).AsXRead()
			if err != nil {
				panic(err)
			}
			id = message["chat"][0].ID
		}
		if err != nil {
			fmt.Println(err.Error())
		}
		parsedMessage := message["chat"][0].FieldValues["writer"]
		messages = append(messages, parsedMessage)
	}
}

func buildStreamIDMap(filePath string) map[string]string {
	var lastestStreamIDs = make(map[string]string)
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	data := make([]byte, 100)
	for {
		n, err := file.Read(data)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if n == 0 {
			break
		}
	}

	r := csv.NewReader(strings.NewReader(string(data)))
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		if len(record) > 1 {
			lastestStreamIDs[record[0]] = strings.TrimSpace(record[1])
		}
	}
	fmt.Println("Stream ID Map: ", lastestStreamIDs)
	file.Close()
	return lastestStreamIDs
}

func saveStreamIDs(streamLatestIDs map[string]string) {
	streamIDsArray := make([][]string, 0)
	for key, value := range streamLatestIDs {
		streamIDsArray = append(streamIDsArray, []string{key, strings.TrimSpace(value)})
	}

	file, err := os.OpenFile(filePath, os.O_RDWR, 0660)
	if err != nil {
		panic(err)
	}
	w := csv.NewWriter(file)

	for _, record := range streamIDsArray {
		if err := w.Write(record); err != nil {
			panic(err)
		}
	}

	//write buffered data to the underlying writer
	w.Flush()

	//check for errors
	if err := w.Error(); err != nil {
		panic(err)
	}
}
