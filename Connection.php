<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace yii\vertica;

use Yii;
use yii\base\Component;
use yii\base\InvalidConfigException;
use yii\base\InvalidParamException;
use yii\helpers\Json;

/**
 * elasticsearch Connection is used to connect to an elasticsearch cluster version 0.20 or higher
 *
 * @property string $driverName Name of the DB driver. This property is read-only.
 * @property boolean $isActive Whether the DB connection is established. This property is read-only.
 * @property QueryBuilder $queryBuilder This property is read-only.
 *
 * @author Vitaliy Zarubint <keygenqt@gmail.com>
 * @since 2.0
 */
class Connection extends Component
{
    /**
     * @event Event an event that is triggered after a DB connection is established
     */
    const EVENT_AFTER_OPEN = 'afterOpen';

    public static $db;

    public $dsn;
    public $username;
    public $password;
    
    private $_table;
    private $_resource;
    
    public function getDb()
    {
        if (self::$db === null && $this->activeConnect) {
            self::$db = $this->exec('SELECT database_name FROM databases')->scalar();
        }
        return self::$db;
    }
    
    /**
     * @param type $id
     * @return \yii\vertica\Connection
     */
    public function setTableId($id)
    {
        $this->_table = $this->getTableName($id);
        return $this;
    }
    
    public function getTableName($id)
    {
        return $this->exec('SELECT table_name FROM tables WHERE table_id=' . QueryBuilder::preparationValue($id))->scalar();
    }

    public function setTable($table)
    {
        $this->_table = $table;
    }
    
    public function getTable()
    {
        return $this->_table;
    }
    
    /**
     * 
     * @param type $sql
     * @return \yii\vertica\Connection
     */
    public function exec($sql)
    {
        $this->open();
        $this->_resource = odbc_exec($this->activeConnect, $sql);
        return $this;
    }
    
    public function execute($sql, $params = [])
    {
        $stmt = odbc_prepare($this->activeConnect, $sql);
        return odbc_execute($stmt, $params);
    }

    public function resource()
    {
        return $this->_resource;
    }
    
    public function one()
    {
        return odbc_fetch_array($this->_resource);
    }
    
    public function scalar()
    {
        if ($value = odbc_fetch_array($this->_resource)) {
            return array_shift($value);
        }
        return null;
    }
    
    public function all()
    {
        $result = [];
        while ($row = odbc_fetch_array($this->_resource)) {
            $result[] = $row;
        }
        return $result;
    }

    /**
     * @var array the active node. key of [[nodes]]. Will be randomly selected on [[open()]].
     */
    public $activeConnect;


    /**
     * Returns a value indicating whether the DB connection is established.
     * @return boolean whether the DB connection is established
     */
    public function getIsActive()
    {
        $this->open();
        return $this->activeConnect !== null;
    }

    /**
     * Establishes a DB connection.
     * It does nothing if a DB connection has already been established.
     * @throws Exception if connection fails
     */
    public function open()
    {
        if ($this->activeConnect !== null) {
            return;
        }
        try {   
            $this->activeConnect = odbc_connect($this->dsn, $this->username, $this->password);
        } catch (yii\base\ErrorException $ex) {
            throw new InvalidConfigException($ex->getMessage());
        }
        Yii::trace('Opening connection to vertica.', __CLASS__);
        $this->initConnection();
    }
    
    public function isConnect($dsn, $username, $password, $error = false)
    {
        try {
            odbc_connect($dsn, $username, $password);
        } catch (\yii\base\ErrorException $ex) {
            if ($error) {
                return $ex->getMessage();
            }
            return false;
        }
        return !$error;
    }

    /**
     * Closes the currently active DB connection.
     * It does nothing if the connection is already closed.
     */
    public function close()
    {
        Yii::trace('Closing connection to vertica.', __CLASS__);
        
        odbc_close($this->activeConnect);
        $this->activeConnect = null;
    }

    /**
     * Initializes the DB connection.
     * This method is invoked right after the DB connection is established.
     * The default implementation triggers an [[EVENT_AFTER_OPEN]] event.
     */
    protected function initConnection()
    {
        $this->trigger(self::EVENT_AFTER_OPEN);
    }

    /**
     * Returns the name of the DB driver for the current [[dsn]].
     * @return string name of the DB driver
     */
    public function getDriverName()
    {
        return 'vertica';
    }

    /**
     * Creates a command for execution.
     * @param array $config the configuration for the Command class
     * @return Command the DB command
     */
    public function createCommand($config = [])
    {
        if (empty($config['db'])) {
            $this->open();
            $config['db'] = $this;
        }
        $command = new Command($config);

        return $command;
    }

    /**
     * Creates new query builder instance
     * @return QueryBuilder
     */
    public function getQueryBuilder()
    {
        return new QueryBuilder($this);
    }
    
    public function quoteColumnName($name)
    {
        return $name;
    }
    
    public function quoteTableName($name)
    {
        return $name;
    }

    /**
     * Performs GET HTTP request
     *
     * @param string $url URL
     * @param array $options URL options
     * @param string $body request body
     * @param boolean $raw if response body contains JSON and should be decoded
     * @return mixed response
     * @throws Exception
     * @throws \yii\base\InvalidConfigException
     */
    public function select($sql)
    {
        $this->open();
        return odbc_exec($this->activeConnect, $sql);
    }
    
    public function update($table, $pkName, $pkValue, $attributes)
    {
        $set = [];
        foreach ($attributes as $key => $value) {
            if ($pkName == $key) {
                continue;
            }
            $value = QueryBuilder::preparationValue($value);
            $set[] = "$key=$value";
        }
        
        $stmt = odbc_prepare($this->activeConnect, "UPDATE $table SET " . implode(', ', $set) . " WHERE $pkName=$pkValue");
        return odbc_execute($stmt);
    }
}
