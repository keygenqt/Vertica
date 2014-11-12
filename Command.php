<?php

namespace yii\vertica;

use yii\base\Component;

/**
 * @author Vitaliy Zarubint <keygenqt@gmail.com>
 * @since 2.0
 */
class Command extends Component
{
    /**
     * @var Connection
     */
    public $db;
    
    /**
     * @var array
     */
    public static $attributes;
    
    /**
     * @var array
     */
    public static $pk;
    
    private $_sql;

    public function __construct($config = []) 
    {
        if (isset($config['sql'])) {
            $this->_sql = $config['sql'];
        }
        if (isset($config['db'])) {
            $this->db = $config['db'];
        }
    }

    /**
     * @return array
     */
    public function search()
    {
        return $this->db->exec($this->_sql)->all();
    }
    
    /**
     * @return array columns model
     */
    public function getColumn()
    {
        if (!isset(self::$attributes[$this->db->table])) {
            $resource = $this->db->exec('SELECT column_name FROM COLUMNS WHERE table_name=' . QueryBuilder::preparationValue($this->db->table))->resource();
            self::$attributes[$this->db->table] = [];
            while ($row = odbc_fetch_array($resource)) {
                self::$attributes[$this->db->table][] = $row['column_name'];
            }
        }
        return self::$attributes[$this->db->table];
    }
    
    /**
     * @return array
     */
    public function getColumnData()
    {
        return $this->db->exec('SELECT * FROM COLUMNS WHERE table_name=' . QueryBuilder::preparationValue($this->db->table))->all();
    }
    
    /**
     * @return string pk name or first int
     */
    public function getPk()
    {
        if (!isset(self::$pk[$this->db->table])) {
            $resource = $this->db->exec('SELECT is_identity, column_name, data_type FROM COLUMNS WHERE table_name=' . QueryBuilder::preparationValue($this->db->table))->resource();
            self::$pk[$this->db->table] = '';
            while ($row = odbc_fetch_array($resource)) {
                if (empty(self::$pk[$this->db->table]) && $row['data_type'] == 'int') {
                    self::$pk[$this->db->table] = $row['column_name'];
                }
                if ($row['is_identity']) {
                    self::$pk[$this->db->table] = $row['column_name'];
                    break;
                }
            }
        }
        return self::$pk[$this->db->table];
    }
    
    /**
     * @return array with data tables
     */
    public function getTables()
    {
        return $this->db->exec('SELECT * FROM tables')->all();
    }

    /**
     * @param string $table
     * @param array $columns
     * @return \yii\vertica\Command
     */
    public function insert($table, $columns)
    {
        $this->_sql = "INSERT INTO $table (" . implode(', ', array_keys($columns)) . ")";
        $values = [];
        foreach ($columns as $key => $value) {
            $values[] = QueryBuilder::preparationValue($value);
        }
        $this->_sql .= ' VALUES (' . implode(', ', $values) . ')';
        return $this;
    }
    
    /**
     * @param type $table
     * @param type $pkName
     * @param type $pkValue
     * @param type $attributes
     * @return boolean
     */
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
        $this->_sql = "UPDATE $table SET " . implode(', ', $set) . " WHERE $pkName=$pkValue";
        $this->db->execute($this->_sql);
        return true;
    }

    /**
     * @param string $table
     * @param mixed $condition array or string
     * @return \yii\vertica\Command
     */
    public function delete($table = null, $condition = '')
    {
        if (empty($table) && !empty($this->_sql)) {
            $this->_sql = 'DELETE ' . substr($this->_sql, strpos($this->_sql, 'FROM'));
        } else {
            if (is_array($condition)) {
                $params = [];
                foreach ($condition as $key => $value) {
                    $params[] = $key . '=' . QueryBuilder::preparationValue($value);
                }
                $this->_sql = "DELETE FROM $table WHERE " . implode(' AND ', $params);
            } else {
                $this->_sql = "DELETE FROM $table WHERE $condition";
            }
        }
        return $this;
    }

    /**
     * @param string $table
     * @param array $columns
     * @param string $options
     * @return \yii\vertica\Command
     */
    public function createTable($table, $columns = [], $options = null)
    {
        $cols = [];
        foreach ($columns as $name => $type) {
            if (is_string($name)) {
                $cols[] = "\t" . $name . ' ' . $type;
            } else {
                $cols[] = "\t" . $type;
            }
        }
        $sql = "CREATE TABLE " . $table . " (\n" . implode(",\n", $cols) . "\n)";
        
        $this->_sql = $options === null ? $sql : $sql . ' ' . $options;
        
        return $this;
    }
    
    /**
     * @param string $table
     * @return \yii\vertica\Command
     */
    public function dropTable($table)
    {
        $this->_sql = "DROP TABLE $table";
        return $this;
    }
    
    /**
     * @return array
     */
    public function queryOne()
    {
        return $this->db->exec($this->_sql)->one();
    }

    /**
     * @return array
     */
    public function queryAll()
    {
        return $this->db->exec($this->_sql)->all();
    }
    
    /**
     * @return string
     */
    public function queryScalar()
    {
        return $this->db->exec($this->_sql)->scalar();
    }

    public function execute($params = [])
    {
        $this->db->execute($this->_sql);
    }

}
