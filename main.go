package main

//This will be the subscriber program
import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/valkey-io/valkey-go"
)

var messages = make([]string, 0)

// tview
var app = tview.NewApplication()
var messagesList = tview.NewList().ShowSecondaryText(false).SetMainTextColor(tcell.ColorGreen)
var flex = tview.NewFlex()
var inputBox = tview.NewInputField().SetLabel("Enter a Message: ").SetFieldTextColor(tcell.ColorGreen)

var lastestStreamIDs map[string]string

func main() {
	//set up flexbox and set it as root
	messagesList.SetBorderPadding(0, 0, 2, 0)
	messagesList.SetSelectedStyle(tcell.StyleDefault)
	reader := bufio.NewScanner(os.Stdin)
	fmt.Print("Please enter your username: ")
	reader.Scan()
	username := reader.Text()
	fmt.Println(username)
	fmt.Print("Please enter your password: ")
	reader.Scan()
	password := reader.Text()
	// fmt.Println(username, password)

	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"192.168.50.238:6379"}, Username: username, Password: password, DisableCache: true})
	if err != nil {
		println("Client error")
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()

	flex.SetDirection(tview.FlexRow).
		AddItem(messagesList, 0, 6, true).
		AddItem(tview.NewFlex().
			// AddItem(tview.NewTextView().SetTextColor(tcell.ColorGreen).SetText("Send message"), 0, 1, false).
			AddItem(inputBox, 0, 6, false), 0, 1, false).SetBorder(true)
	inputBox.SetDoneFunc(func(key tcell.Key) {
		newMessage := username + ": " + inputBox.GetText()
		err := client.Do(ctx, client.B().Publish().Channel("chat").Message(newMessage).Build()).Error()
		if err != nil {
			panic(err.Error())
		}
		inputBox.SetText("")
	})
	// flex.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
	// 	if event.Rune() == 32 {
	// 		//send message
	// 		//write send message function
	// 		//publish to channel
	// 		go sendMessage(ctx, client, username, inputBox.GetText())
	// 	}
	// 	return event
	// })

	//spin off message receving into it's own go routine
	//this way we will still recieve messages
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
	// err = client.Receive(ctx, client.B().Subscribe().Channel(username, "news").Build(), func(msg valkey.PubSubMessage) {
	// 	// fmt.Printf(msg.Channel, ":", msg.Message)
	// 	fmt.Printf("%v: %v\n", msg.Channel, msg.Message)
	// 	messages = append(messages, msg.Message)
	// })
	// fmt.Println(err.Error())

	// err = client.Receive(ctx, client.B().Subscribe().Channel("news").Build(), func(msg valkey.PubSubMessage) {
	// 	fmt.Printf(msg.Channel, ":", msg.Message)
	// })
}

func receiveMessages(ctx context.Context, client valkey.Client, app *tview.Application) {
	id := ""
	var message map[string][]valkey.XRangeEntry
	var err error
	for {
		if id == "" {
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
		messagesList.AddItem(parsedMessage, "", rune(0), nil)
		app.Draw()
	}
	// err := client.Receive(ctx, client.B().Subscribe().Channel("chat").Build(), func(msg valkey.PubSubMessage) {
	// 	messages = append(messages, msg.Message)
	// 	messagesList.AddItem(msg.Message, "", rune(0), nil)
	// 	app.Draw()
	// })
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
			// parsedMessage = message["chat"][0].FieldValues["writer"]
			id = message["chat"][0].ID
		} else {
			message, err = client.Do(ctx, client.B().Xread().Block(0).Streams().Key(stream).Id(id).Build()).AsXRead()
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
		messagesList.AddItem(parsedMessage, "", rune(0), nil)
	}
}

func updateMessages() {
	for index, message := range messages {
		messagesList.AddItem(message, " ", rune(49+index), nil)
	}
}

// func sendMessage(ctx context.Context, client valkey.Client, username string, message string) {
// 	err := client.Do(ctx, client.B().Publish().Channel("lyam").Message(message).Build()).Error()
// 	inputBox.SetDoneFunc()
// }
