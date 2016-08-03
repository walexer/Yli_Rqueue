<?php 

class Yli_Rqueue_Model_Redis extends Zend_Queue_Adapter_AdapterAbstract
{
    const DEFAULT_HOST = '127.0.0.1';
    const DEFAULT_PORT = 6379;
    const QUEUE_KEY = 'queue';
    
    /**
     * @var 
     */
    private $_client = null;
    
    
    /**
     * Constructor
     *
     * @param  array|Zend_Config $config An array having configuration data
     * @param  Zend_Queue The Zend_Queue object that created this class
     * @return void
    */
    public function __construct($options, Zend_Queue $queue = null)
    {
        if(!$this->_client){
            $host = $options['host'];
            $port = $options['port'];
            $db = $options['db'];
            $this->_client = new Credis_Client($host,$port?$port:self::DEFAULT_PORT);
            $this->_client->select($db);
        }
        return $this->_client;
    }
    
    /**
     * Create a new queue
     *
     * @param  string  $name    queue name
     * @param  integer $timeout default visibility timeout
     * @return void
     * @throws Zend_Queue_Exception
     */
    public function create($name, $timeout=null)
    {
        #require_once 'Zend/Queue/Exception.php';
        throw new Zend_Queue_Exception('create() is not supported in ' . get_class($this));
    }
    
    /**
     * Delete a queue and all of its messages
     *
     * @param  string $name queue name
     * @return void
     * @throws Zend_Queue_Exception
     */
    public function delete($name)
    {
        #require_once 'Zend/Queue/Exception.php';
        throw new Zend_Queue_Exception('delete() is not supported in ' . get_class($this));
    }
    
    /**
     * Push an element onto the end of the queue
     *
     * @param  string     $message message to send to the queue
     * @param  Zend_Queue $queue
     * @return Zend_Queue_Message
     */
    public function send($message, Zend_Queue $queue=null)
    {
        $this->_client->lPush(self::QUEUE_KEY, $message);
    }
    
    /**
     * Return the first element in the queue
     *
     * @param  integer    $maxMessages
     * @param  integer    $timeout
     * @param  Zend_Queue $queue
     * @return Zend_Queue_Message_Iterator
     */
    public function receive($maxMessages=null, $timeout=null, Zend_Queue $queue=null)
    {
        if ($maxMessages === null) {
            $maxMessages = 1;
        }

        if ($timeout === null) {
            $timeout = self::RECEIVE_TIMEOUT_DEFAULT;
        }
        $socket_timeout = ini_get('default_socket_timeout');
        if($timeout >= $socket_timeout){
            $timeout = $socket_timeout - 2;
        }
        if ($queue === null) {
            $queue = $this->_queue;
        }
        
        $msgs = array();
        if ($maxMessages > 0 ) {
            for ($i = 0; $i < $maxMessages; $i++) {
                $data = array(
                    'handle' => md5(uniqid(rand(), true)),
                    'body'   => $this->_client->brPop(self::QUEUE_KEY,$timeout),
                );
        
                $msgs[] = $data;
            }
        }
        
        $options = array(
            'queue'        => $queue,
            'data'         => $msgs,
            'messageClass' => $queue->getMessageClass(),
        );
        
        $classname = $queue->getMessageSetClass();
        if (!class_exists($classname)) {
            #require_once 'Zend/Loader.php';
            Zend_Loader::loadClass($classname);
        }
        return new $classname($options);
        
    }
    
    /**
     * Delete a message from the queue
     *
     * Returns true if the message is deleted, false if the deletion is
     * unsuccessful.
     *
     * @param  Zend_Queue_Message $message
     * @return boolean
     * @throws Zend_Queue_Exception (unsupported)
     */
    public function deleteMessage(Zend_Queue_Message $message)
    {
        #require_once 'Zend/Queue/Exception.php';
        throw new Zend_Queue_Exception('deleteMessage() is not supported in  ' . get_class($this));
    }
    
    /**
     * Get an array of all available queues
     *
     * @return void
     * @throws Zend_Queue_Exception
     */
    public function getQueues()
    {
        #require_once 'Zend/Queue/Exception.php';
        throw new Zend_Queue_Exception('getQueues() is not supported in this adapter');
    }
    
    /**
     * Returns the length of the queue
     *
     * @param  Zend_Queue $queue
     * @return integer
     * @throws Zend_Queue_Exception (not supported)
     */
    public function count(Zend_Queue $queue=null)
    {
        #require_once 'Zend/Queue/Exception.php';
        throw new Zend_Queue_Exception('count() is not supported in this adapter');
    }
    
    /**
     * Does a queue already exist?
     *
     * @param  string $name
     * @return boolean
     * @throws Zend_Queue_Exception (not supported)
     */
    public function isExists($name)
    {
        #require_once 'Zend/Queue/Exception.php';
        throw new Zend_Queue_Exception('isExists() is not supported in this adapter');
    }
    
    
    public function getCapabilities()
    {
        return array(
            'create'        => false,
            'delete'        => false,
            'send'          => true,
            'receive'       => true,
            'deleteMessage' => false,
            'getQueues'     => false,
            'count'         => false,
            'isExists'      => false,
        );
    }
}