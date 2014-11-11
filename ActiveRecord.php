<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace yii\vertica;

use Yii;
use yii\base\NotSupportedException;
use yii\db\BaseActiveRecord;
use yii\helpers\ArrayHelper;
use yii\helpers\Json;

class ActiveRecord extends BaseActiveRecord
{
    public static $primary = [];

    /**
     * @return Connection the database connection used by this AR class.
     */
    public static function getDb()
    {
        /* @var $db \yii\vertica\Connection */
        $called = get_called_class();
        $db = \Yii::$app->get('vertica');
        $table = $called::tableName();
        if (is_numeric($table)) {
            $db->setTable($db->getTableName($table));
        } else {
            $db->setTable($table);
        }
        return $db;
    }

    /**
     * @inheritdoc
     * @return ActiveQuery the newly created [[ActiveQuery]] instance.
     */
    public static function find()
    {
        $called = get_called_class();
        $query = Yii::createObject(ActiveQuery::className(), [$called]);
        $query->from = $called::tableName();
        return $query;
    }

    /**
     * @inheritdoc
     */
    public static function findOne($condition)
    {
        $query = static::find();
        if (is_array($condition)) {
            return $query->andWhere($condition)->one();
        } else {
            return static::get($condition);
        }
    }

    /**
     * @inheritdoc
     */
    public static function findAll($condition)
    {
        $query = static::find();
        if (ArrayHelper::isAssociative($condition)) {
            return $query->andWhere($condition)->all();
        } else {
            return static::mget((array) $condition);
        }
    }

    /**
     * @inheritdoc
     */
    public function getPrimaryKey($asArray = false)
    {
        $pk = static::primaryKey()[0];
        if ($asArray) {
            return [$pk => $this->$pk];
        } else {
            return $this->$pk;
        }
    }

    public static function primaryKey()
    {
        $class = get_called_class();
        if (!isset(self::$primary[$class])) {
            self::$primary[$class] = self::getDb()->createCommand()->getPk();
        }
        return [self::$primary[$class]];
    }

    public function attributes()
    {
        return self::getDb()->createCommand()->getColumn();
    }

    public function arrayAttributes()
    {
        return self::getDb()->createCommand()->getColumn();
    }
    
    public function safeAttributes()
    {
        return self::getDb()->createCommand()->getColumn();
    }

    /**
     * @inheritdoc
     *
     * @param ActiveRecord $record the record to be populated. In most cases this will be an instance
     * created by [[instantiate()]] beforehand.
     * @param array $row attribute values (name => value)
     */
    public static function populateRecord($record, $row)
    {
        $attributes = [];
        if (!empty($row)) {
            $attributes = $row;
        }
        if (!empty($row)) {
            // reset fields in case it is scalar value
            $arrayAttributes = $record->arrayAttributes();
            foreach($row as $key => $value) {
                if (!isset($arrayAttributes[$key])) {
                    $row[$key] = $value;
                }
            }
            $attributes = array_merge($attributes, $row);
        }
        parent::populateRecord($record, $attributes);
    }

    /**
     * Creates an active record instance.
     *
     * This method is called together with [[populateRecord()]] by [[ActiveQuery]].
     * It is not meant to be used for creating new records directly.
     *
     * You may override this method if the instance being created
     * depends on the row data to be populated into the record.
     * For example, by creating a record based on the value of a column,
     * you may implement the so-called single-table inheritance mapping.
     * @param array $row row data to be populated into the record.
     * This array consists of the following keys:
     *  - `_source`: refers to the attributes of the record.
     *  - `_type`: the type this record is stored in.
     *  - `_index`: the index this record is stored in.
     * @return static the newly created active record
     */
    public static function instantiate($row)
    {
        return new static;
    }

    /**
     * Inserts a document into the associated index using the attribute values of this record.
     *
     * This method performs the following steps in order:
     *
     * 1. call [[beforeValidate()]] when `$runValidation` is true. If validation
     *    fails, it will skip the rest of the steps;
     * 2. call [[afterValidate()]] when `$runValidation` is true.
     * 3. call [[beforeSave()]]. If the method returns false, it will skip the
     *    rest of the steps;
     * 4. insert the record into database. If this fails, it will skip the rest of the steps;
     * 5. call [[afterSave()]];
     *
     * In the above step 1, 2, 3 and 5, events [[EVENT_BEFORE_VALIDATE]],
     * [[EVENT_BEFORE_INSERT]], [[EVENT_AFTER_INSERT]] and [[EVENT_AFTER_VALIDATE]]
     * will be raised by the corresponding methods.
     *
     * Only the [[dirtyAttributes|changed attribute values]] will be inserted into database.
     *
     * If the [[primaryKey|primary key]] is not set (null) during insertion,
     * it will be populated with a
     * [randomly generated value](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-index_.html#_automatic_id_generation)
     * after insertion.
     *
     * For example, to insert a customer record:
     *
     * ~~~
     * $customer = new Customer;
     * $customer->name = $name;
     * $customer->email = $email;
     * $customer->insert();
     * ~~~
     *
     * @param boolean $runValidation whether to perform validation before saving the record.
     * If the validation fails, the record will not be inserted into the database.
     * @param array $attributes list of attributes that need to be saved. Defaults to null,
     * meaning all attributes will be saved.
     * @param array $options options given in this parameter are passed to elasticsearch
     * as request URI parameters. These are among others:
     *
     * - `routing` define shard placement of this record.
     * - `parent` by giving the primaryKey of another record this defines a parent-child relation
     * - `timestamp` specifies the timestamp to store along with the document. Default is indexing time.
     *
     * Please refer to the [elasticsearch documentation](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-index_.html)
     * for more details on these options.
     *
     * By default the `op_type` is set to `create`.
     * @return boolean whether the attributes are valid and the record is inserted successfully.
     */
    public function insert($runValidation = true, $attributes = null, $options = ['op_type' => 'create'])
    {
        if ($runValidation && !$this->validate($attributes)) {
            return false;
        }
        if (!$this->beforeSave(true)) {
            return false;
        }
        
        $values = $this->getDirtyAttributes($attributes);
        
        static::getDb()->createCommand()->insert(
            static::tableName(),
            $values
        )->execute();

        $changedAttributes = array_fill_keys(array_keys($values), null);
        $this->setOldAttributes($values);
        $this->afterSave(true, $changedAttributes);

        return true;
    }

    /**
     * Updates all records whos primary keys are given.
     * For example, to change the status to be 1 for all customers whose status is 2:
     *
     * ~~~
     * Customer::updateAll(['status' => 1], [2, 3, 4]);
     * ~~~
     *
     * @param array $attributes attribute values (name-value pairs) to be saved into the table
     * @param array $condition the conditions that will be put in the WHERE part of the UPDATE SQL.
     * Please refer to [[ActiveQuery::where()]] on how to specify this parameter.
     * @return integer the number of rows updated
     * @throws Exception on error.
     */
    public static function updateAll($attributes, $condition = [])
    {
        $pkName = static::primaryKey()[0];
        if (empty($condition[$pkName])) {
            return 0;
        }
        $class = get_called_class();
        return self::getDb()->update($class::tableName(), $pkName, $condition[$pkName], $attributes);
    }

    /**
     * Updates all matching records using the provided counter changes and conditions.
     * For example, to increment all customers' age by 1,
     *
     * ~~~
     * Customer::updateAllCounters(['age' => 1]);
     * ~~~
     *
     * @param array $counters the counters to be updated (attribute name => increment value).
     * Use negative values if you want to decrement the counters.
     * @param string|array $condition the conditions that will be put in the WHERE part of the UPDATE SQL.
     * Please refer to [[Query::where()]] on how to specify this parameter.
     * @return integer the number of rows updated
     * @throws Exception on error.
     */
    public static function updateAllCounters($counters, $condition = [])
    {
        $pkName = static::primaryKey()[0];
        if (count($condition) == 1 && isset($condition[$pkName])) {
            $primaryKeys = is_array($condition[$pkName]) ? $condition[$pkName] : [$condition[$pkName]];
        } else {
            $primaryKeys = static::find()->where($condition)->column($pkName); // TODO check whether this works with default pk _id
        }
        if (empty($primaryKeys) || empty($counters)) {
            return 0;
        }
        $bulk = '';
        foreach ($primaryKeys as $pk) {
            $action = Json::encode([
                "update" => [
                    "_id" => $pk,
                    "_type" => static::type(),
                    "_index" => static::index(),
                ],
            ]);
            $script = '';
            foreach ($counters as $counter => $value) {
                $script .= "ctx._source.$counter += $counter;\n";
            }
            $data = Json::encode([
                "script" => $script,
                "params" => $counters
            ]);
            $bulk .= $action . "\n" . $data . "\n";
        }

        // TODO do this via command
        $url = [static::index(), static::type(), '_bulk'];
        $response = static::getDb()->post($url, [], $bulk);
        $n = 0;
        $errors = [];
        foreach ($response['items'] as $item) {
            if (isset($item['update']['status']) && $item['update']['status'] == 200) {
                $n++;
            } else {
                $errors[] = $item['update'];
            }
        }
        if (!empty($errors) || isset($response['errors']) && $response['errors']) {
            throw new Exception(__METHOD__ . ' failed updating records counters.', $errors);
        }

        return $n;
    }

    /**
     * Deletes rows in the table using the provided conditions.
     * WARNING: If you do not specify any condition, this method will delete ALL rows in the table.
     *
     * For example, to delete all customers whose status is 3:
     *
     * ~~~
     * Customer::deleteAll('status = 3');
     * ~~~
     *
     * @param array $condition the conditions that will be put in the WHERE part of the DELETE SQL.
     * Please refer to [[ActiveQuery::where()]] on how to specify this parameter.
     * @return integer the number of rows deleted
     * @throws Exception on error.
     */
    public static function deleteAll($condition = [])
    {
        $pkName = static::primaryKey()[0];
        if (count($condition) == 1 && isset($condition[$pkName])) {
            $primaryKeys = is_array($condition[$pkName]) ? $condition[$pkName] : [$condition[$pkName]];
        } else {
            $primaryKeys = static::find()->where($condition)->column($pkName); // TODO check whether this works with default pk _id
        }
        if (empty($primaryKeys)) {
            return 0;
        }
        $bulk = '';
        foreach ($primaryKeys as $pk) {
            $bulk .= Json::encode([
                "delete" => [
                    "_id" => $pk,
                    "_type" => static::type(),
                    "_index" => static::index(),
                ],
            ]) . "\n";
        }

        // TODO do this via command
        $url = [static::index(), static::type(), '_bulk'];
        $response = static::getDb()->post($url, [], $bulk);
        $n = 0;
        $errors = [];
        foreach ($response['items'] as $item) {
            if (isset($item['delete']['status']) && $item['delete']['status'] == 200) {
                if (isset($item['delete']['found']) && $item['delete']['found']) {
                    $n++;
                }
            } else {
                $errors[] = $item['delete'];
            }
        }
        if (!empty($errors) || isset($response['errors']) && $response['errors']) {
            throw new Exception(__METHOD__ . ' failed deleting records.', $errors);
        }

        return $n;
    }

    /**
     * Destroys the relationship in current model.
     *
     * This method is not supported by elasticsearch.
     */
    public function unlinkAll($name, $delete = false)
    {
        throw new NotSupportedException('unlinkAll() is not supported by elasticsearch, use unlink() instead.');
    }
}
