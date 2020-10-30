<?php


namespace lliytnik\eventbus\transports;


use lliytnik\eventbus\events\Event;

class KafkaTransport extends Transport
{

    function send(Event $event)
    {
        // TODO: Implement send() method.
    }

    function consume()
    {
        // TODO: Implement consume() method.
    }

    function encodeEvent(Event $event)
    {
        // TODO: Implement encodeEvent() method.
    }

    function decodeEvent(Event $event)
    {
        // TODO: Implement decodeEvent() method.
    }
}