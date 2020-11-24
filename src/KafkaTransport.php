<?php


namespace lliytnik\eventbus\transports;


use lliytnik\eventbus\events\Event;
use lliytnik\eventbus\transports\Transport;
use lliytnik\kafka\Consumer;
use lliytnik\kafka\Producer;
use lliytnik\kafka\Sender;

class KafkaTransport extends Transport
{
    public $kafka;
    public $config;
    public $producer;
    public $consumer;

    public function __construct(array $config)
    {
        $this->config = $config;
    }

    function send(Event $event)
    {
        if(!$this->producer){
            $this->initProducer();
        }
        $this->producer->send($this->encodeEvent($event));
    }

    function consume()
    {
        if(!$this->consumer){
            $this->initConsumer();
        }
        $this->consumer->consume();
    }

    function encodeEvent(Event $event)
    {
        return json_encode($event);
    }

    function decodeEvent(string $event)
    {
        return json_decode($event);
    }

    protected function initProducer(){
        $this->producer = new Producer($this->config);
    }

    protected function initConsumer(){
        $this->consumer = new Consumer($this->config);
    }

    public function setMessageCallBack($messageCallBack){
        $this->consumer->messageCallBack = $messageCallBack;
    }

    public function setErrorCallBack($errorCallBack){
        $this->consumer->errorCallBack = $errorCallBack;
    }

    function getConsumer()
    {
        if(!$this->consumer){
            $this->initConsumer();
        }
        return $this->consumer;
    }

    function getProducer()
    {
        if(!$this->getProducer()){
            $this->initProducer();
        }
        return $this->producer;
    }
}