<?php

namespace yii\vertica;

use yii\base\InvalidConfigException;

class Migration extends \yii\db\Migration
{
    public $db = 'vertica';

    /**
     * Initializes the migration.
     */
    public function init()
    {
        $connection = Connection::className();
        if (!($this->db instanceof $connection)) {
            throw new InvalidConfigException('The required component is not specified.');
        }
    }
}
