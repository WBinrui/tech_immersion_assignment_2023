package main

import (
	"context"
    "database/sql"

	"github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc"
	_ "github.com/mattn/go-sqlite3"
	"fmt"
	//"log"
	"time"
	"strings"
)

// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct{}

func sortChatName(chatName string, senderName string) (string, error) { //validates chat matches sender, also returns sorted version of chat name
	chatNameSplit := strings.Split(chatName, ":")

	if len(chatNameSplit) != 2 {
		err := fmt.Errorf("chat \"%s\" is in incorrect format", chatName)
		return "", err
	}

	user1, user2 := chatNameSplit[0], chatNameSplit[1]

	if senderName != "" { // ignore if sender is not specified
		if !(senderName == user1 || senderName == user2) {
			err := fmt.Errorf("sender \"%s\" not in requested chat \"%s\"", senderName, chatName)
			return "", err
		}
	}
	
	var result string // will be sorted version of input chatName

	if user1 > user2 {
		result = fmt.Sprintf("%s:%s", user2, user1) //switch if not in lexical order
	} else {
		result = fmt.Sprintf("%s:%s", user1, user2) //allow same user - send to self
	}

	return result, nil
}

func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {

	fmt.Println("send request test:\n", req)
	resp := rpc.NewSendResponse()

    db, err := sql.Open("sqlite3", "MessageDatabase.db")

    if err != nil {
		return nil, err
    }

    defer db.Close()

    sender := req.Message.Sender
    chat, err := sortChatName(req.Message.Chat, sender) // verifies chat matches sender and sorts users in chat name to avoid creating duplicate chats

    if err != nil {
		return nil, err
    }

    text := req.Message.Text
    timestamp := time.Now().Unix()

    _, err = db.Exec("INSERT INTO message_history ('chat', 'text', 'sender', 'timestamp') VALUES (?, ?, ?, ?)", chat, text, sender, timestamp)

    if err != nil {
		return nil, err
    }

	resp.Code, resp.Msg = 0, "success"
	return resp, nil
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {

	resp := rpc.NewPullResponse()
	db, err := sql.Open("sqlite3", "MessageDatabase.db")

    if err != nil {
		return nil, err
    }

    defer db.Close()

    chat, err := sortChatName(req.Chat, "")

    if err != nil {
		return nil, err
    }

    cursor := req.Cursor
    actualLimit := int(req.Limit + 1) // +1 extra row to check for HasMore
    reverse := req.Reverse

    //fmt.Println("%s %d %d %b pull test", chat, cursor, actualLimit, reverse)

    var queryString string

    fmt.Println(chat, req)
    if *reverse {
    	queryString = "SELECT * FROM message_history WHERE chat=? AND timestamp<=? ORDER BY timestamp DESC LIMIT ?"
    } else {
    	queryString = "SELECT * FROM message_history WHERE chat=? AND timestamp>=? ORDER BY timestamp ASC LIMIT ?"
    }
    
    rows, err := db.Query(queryString, chat, cursor, actualLimit)

    if err != nil {
		return nil, err
    }

    defer rows.Close()

    rowCount := 0 // number of rows in rows
    hasMore := false // assume more rows do not exist
    var nextCursor int64

    for rows.Next() {

    	var chat string
        var text string
        var sender string
        var timestamp int64

        rowCount ++

        err = rows.Scan(&chat, &text, &sender, &timestamp)

        if err != nil {
			return nil, err
        }

        //fmt.Printf("%s %s %s %d test", chat, text, sender, timestamp)

        // adds all messages within orignal request into response
        if rowCount < actualLimit {
	        resp.Messages = append(resp.Messages, &rpc.Message{
	        	Chat: chat,
	        	Text: text,
	        	Sender: sender,
	        	SendTime: timestamp,
	        })
	    } else {
	    	nextCursor = timestamp // sets nextCursor to timestamp of first row after limit
	    	hasMore = true // hasMore is true if at least one more row exists after limit
	    }
    }

    resp.HasMore = &hasMore
    resp.NextCursor = &nextCursor
	resp.Code, resp.Msg = 0, "success"

	return resp, nil
}