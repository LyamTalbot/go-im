package main

//This will be the subscriber program
import (
	"bufio"
	"context"
	"fmt"
	"os"
	"slices"
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

// tview
var app = tview.NewApplication()
var flex = tview.NewFlex()

var flexRoot = tview.NewFlex()
var messagesBox = tview.NewTextView()
var pages = tview.NewPages()

// map of users (testing purposes just so I gen-generate the stream names and chat window names)
// this will need to be moved to valkey and retreived if we don't have local copy
//
//	var userChatWindows = map[string][]string{
//		"ant":   {"ant:louis", "ant:lyam"},
//		"louis": {"ant:louis", "louis:lyam"},
//		"lyam":  {"ant:lyam", "louis:lyam"},
//	}
var username string

// var messageBoxes = make(map[string]*tview.TextView)
// try using a slice of textView
// var messageBoxes = make(map[string][]*tview.TextView)
// no that's probably a little annoying
// make it a map[string]tview.Flex
var messageBoxes = make(map[string]*tview.Flex)

// var logPath = "./test_log.txt"

func main() {
	// latestStreamIDs = buildStreamIDMap(filePath)
	messagesBox.SetBackgroundColor(tcell.ColorBlack)
	reader := bufio.NewScanner(os.Stdin)
	var password string
	if len(os.Args) > 1 && os.Args[1] != "--debug" {
		fmt.Print("Please enter your username: ")
		reader.Scan()
		username = reader.Text()
		fmt.Println(username)
		fmt.Print("Please enter your password: ")
		reader.Scan()
		password = reader.Text()
	} else {
		username = "lyam"
		password = "0"
	}

	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"192.168.50.238:6379"}, Username: username, Password: password, DisableCache: true})
	if err != nil {
		println("Client error")
		panic(err.Error())
	}
	defer client.Close()

	ctx := context.Background()
	friends, err := client.Do(ctx, client.B().Smembers().Key(fmt.Sprintf("%v:friends", username)).Build()).AsStrSlice()
	chatKeys := make(map[string]string)
	for _, friend := range friends {
		chatKeys[friend], err = client.Do(ctx, client.B().Get().Key(fmt.Sprintf("%v:chats:%v", username, friend)).Build()).ToString()
		if err != nil {
			panic(err)
		}
	}
	if err != nil {
		panic(err)
	}
	conversations := tview.NewList()
	conversations.SetBorder(true)
	for index, friend := range friends {
		conversations.AddItem(friend, "", rune(0), func() {
			pages.SwitchToPage(fmt.Sprintf("%v", index))
		})
	}
	// for index, conversation := range userChatWindows[username] {
	// 	conversations.AddItem(conversation, "", rune(0), func() {
	// 		pages.SwitchToPage(fmt.Sprintf("%v", index))
	// 	})
	// }
	flexRoot.AddItem(conversations, 0, 1, false)
	flexRoot.AddItem(pages, 0, 8, false)
	for page := 0; page < len(friends); page++ {
		func(page int) {
			chatKey := chatKeys[friends[page]]
			flex := tview.NewFlex()
			flex.SetDirection(tview.FlexRow)
			flex.SetTitle(friends[page])
			// messagesBox := tview.NewTextView()
			// messageBoxes[userChatWindows[username][page]] = *tview.NewFlex()
			// messagesBox.SetTextColor(tcell.ColorLightGreen)
			innerFlex := tview.NewFlex()
			innerFlex.SetDirection(tview.FlexRow)
			messageBoxes[chatKey] = innerFlex
			// messageBoxes[userChatWindows[username][page]] = innerFlex
			inputBox := tview.NewInputField().SetLabel("Enter a Message: ").SetFieldTextColor(tcell.ColorGreen)
			inputBox.SetLabelColor(tcell.ColorLightGreen)
			inputBox.SetBackgroundColor(tcell.ColorBlack.TrueColor())
			inputBox.SetFieldBackgroundColor(tcell.ColorBlack.TrueColor())
			inputBox.SetFieldTextColor(tcell.ColorLightGreen)
			inputBox.SetDoneFunc(func(key tcell.Key) {
				message := fmt.Sprintf("Username: %v, Message:%v,Time:%v", username, inputBox.GetText(), time.TimeOnly)
				if err != nil {
					panic(err)
				}
				err = client.Do(ctx, client.B().Xadd().Key(chatKey).Id("*").FieldValue().FieldValue("message", message).Build()).Error()
				if err != nil {
					panic(err)
				}
				inputBox.SetText("")
			})
			// buttons := tview.NewFlex()
			// buttons.AddItem(tview.NewButton("Quit").SetSelectedFunc(func() {
			// 	app.Stop()
			// }), 0, 1, false)
			// buttons.AddItem(tview.NewButton("Next").SetSelectedFunc(func() {
			// 	pages.SwitchToPage(fmt.Sprintf("%v", (page+1)%pages.GetPageCount()))
			// }), 0, 1, false)
			// flex.AddItem(messagesBox, 0, 6, true)
			flex.AddItem(innerFlex, 0, 6, true)
			flex.AddItem(inputBox, 0, 1, false)
			// flex.AddItem(buttons, 0, 1, false)
			flex.SetBorder(true)
			pages.AddPage(fmt.Sprintf("%v", page),
				flex, true, true)
		}(page)
	}

	//spin off message receving into it's own go routine
	//this way we will still recieve messages
	rebuildMessagesList(chatKeys, ctx, client)
	go receiveMessages(chatKeys, ctx, client, app)
	//replace pages with grid?
	if err := app.SetRoot(flexRoot, true).EnableMouse(true).Run(); err != nil {
		panic(err)
	}
	// if err := app.SetRoot(flex, true).EnableMouse(true).Run(); err != nil {
	// 	panic(err)
	// }
	//Set key val NX

	//For subscribing to a channel we could just run this inside it's own go routine and have it update the UI.
	//Based on this we would just subscribe to every channel we are apart of.
	//If you wanted to think about it in terms of Discord servers, each server would be a channel
	//I think other thing that needs to be done would be to subscribe to our "own" channel, any messages that are sent to us from other
	//users should be sent to that user's channel.
	//once they're recieved we handle them, update the correct parts of the UI, store them seperately based on where they're from, etc.
}

func rebuildMessagesList(chatKeys map[string]string, ctx context.Context, client valkey.Client) {
	//I need to do xrevrange for every chat]
	//the chats are stored in <username>_chats.csv
	//but for now I have them hard coded to make it easier
	//this isn't just the name of the friend it's the chat key
	//so I'd need to retreive the chat keys
	for _, chatKey := range chatKeys {
		// var stringBuilder strings.Builder
		// chatKey, err := client.Do(ctx, client.B().Get().Key(chatKey).Build()).ToString()
		// if err != nil {
		// panic(err)
		// }
		valkeyResponse, err := client.Do(ctx, client.B().Xrevrange().Key(chatKey).End("+").Start("-").Count(10).Build()).AsXRange()
		if err != nil {
			panic(err)
		}
		slices.Reverse(valkeyResponse)
		for _, entry := range valkeyResponse {
			//if the value is the empty string we should not write to the string because we end up injecting an empty line
			//should probably flag empty messages to not be sent in the first place
			if entry.FieldValues["message"] == "" {
				continue
			} else {
				messageBox := tview.NewTextView()
				messageBox.SetWordWrap(true)
				messageBox.SetWrap(true)
				messageBox.SetTextColor(tcell.ColorLightGreen)
				messageBox.SetFocusFunc(func() {
					//this was just to experiment and see if I could set focus on the message boxes within the
					//flex container. as it turns out I can.
					//I'll be able to use this later if I want to remove or edit messages because I'll have the index of the message.
					messageBox.SetBackgroundColor(tcell.ColorLightGreen)
					messageBox.SetTextColor(tcell.ColorBlack)
				})
				messageBox.SetBlurFunc(func() {
					messageBox.SetBackgroundColor(tcell.ColorBlack)
					messageBox.SetTextColor(tcell.ColorLightGreen)
				})
				messageBox.SetText(entry.FieldValues["message"])
				messageBox.SetWrap(true)
				messageBox.SetWordWrap(true)
				messageBox.SetBorderPadding(0, 0, 0, 0)
				messageBoxes[chatKey].AddItem(messageBox, 1, 1, false)
				// stringBuilder.WriteString(entry.FieldValues["message"] + "\n")
			}
		}
		// messageBoxes[key].SetText(stringBuilder.String())
		// messageBoxes[key].ScrollToEnd()
	}
}

func receiveMessages(friends map[string]string, ctx context.Context, client valkey.Client, app *tview.Application) {
	keys := make([]string, 0)
	ids := make([]string, 0)
	//Need to make a list of IDs that is the same length as the number of streams we want to read from.
	//I might try to find another way to do this
	//But I'm pretty sure according to the valkey docs we need key_1, key_2, key_3 id_1, id_2, id_3 so I might have to stick with this.
	// chatWindows := userChatWindows[username]
	for _, chatKey := range friends {
		keys = append(keys, chatKey)
		ids = append(ids, "$")
	}
	for {
		response, err := client.Do(ctx, client.B().Xread().Block(0).Streams().Key(keys...).Id(ids...).Build()).AsXRead()
		if err != nil {
			// fmt.Println("Cannot rebuild messages list")
			panic(fmt.Sprintf("Cannot set up receive messages list: %v", err.Error()))
		}
		for chatKey, message := range response {
			textView := tview.NewTextView()
			textView.SetText(message[0].FieldValues["message"])
			textView.SetTextColor(tcell.ColorLightGreen)
			textView.SetBorderPadding(0, 0, 0, 0)
			messageBoxes[chatKey].AddItem(textView, 1, 1, false)
		}
		app.Draw()
	}
}

// func readFromStream(stream string, ctx context.Context, client valkey.Client, app *tview.Application) {
// 	id := ""
// 	var message map[string][]valkey.XRangeEntry
// 	var err error
// 	for {
// 		if id == "" {
// 			message, err = client.Do(ctx, client.B().Xread().Block(0).Streams().Key(stream).Id("$").Build()).AsXRead()
// 			if err != nil {
// 				panic(err)
// 			}
// 			id = message["chat"][0].ID
// 		} else {
// 			message, err = client.Do(ctx, client.B().Xread().Block(0).Streams().Key(stream).Id(id).Build()).AsXRead()
// 			if err != nil {
// 				panic(err)
// 			}
// 			id = message["chat"][0].ID
// 		}
// 		if err != nil {
// 			fmt.Println(err.Error())
// 		}
// 		parsedMessage := message["chat"][0].FieldValues["writer"]
// 		messages = append(messages, parsedMessage)
// 	}
// }

// func buildStreamIDMap(filePath string) map[string]string {
// 	var lastestStreamIDs = make(map[string]string)
// 	file, err := os.Open(filePath)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer file.Close()

// 	data := make([]byte, 100)
// 	for {
// 		n, err := file.Read(data)
// 		if err != nil && err != io.EOF {
// 			panic(err)
// 		}
// 		if n == 0 {
// 			break
// 		}
// 	}

// 	r := csv.NewReader(strings.NewReader(string(data)))
// 	for {
// 		record, err := r.Read()
// 		if err == io.EOF {
// 			break
// 		}
// 		if err != nil {
// 			panic(err)
// 		}
// 		if len(record) > 1 {
// 			lastestStreamIDs[record[0]] = strings.TrimSpace(record[1])
// 		}
// 	}
// 	fmt.Println("Stream ID Map: ", lastestStreamIDs)
// 	file.Close()
// 	return lastestStreamIDs
// }

// func saveStreamIDs(streamLatestIDs map[string]string) {
// 	streamIDsArray := make([][]string, 0)
// 	for key, value := range streamLatestIDs {
// 		streamIDsArray = append(streamIDsArray, []string{key, strings.TrimSpace(value)})
// 	}

// 	file, err := os.OpenFile(filePath, os.O_RDWR, 0660)
// 	if err != nil {
// 		panic(err)
// 	}
// 	w := csv.NewWriter(file)

// 	for _, record := range streamIDsArray {
// 		if err := w.Write(record); err != nil {
// 			panic(err)
// 		}
// 	}

// 	//write buffered data to the underlying writer
// 	w.Flush()

// 	//check for errors
// 	if err := w.Error(); err != nil {
// 		panic(err)
// 	}
// }
