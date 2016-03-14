Vertica Query and ActiveRecord for Yii 2
==============================================

(Beta version. Will have problems, please contact: keygenqt@gmail.com)

This extension provides the [vertica](https://my.vertica.com/vertica-documentation/) integration for the Yii2 framework.
It includes basic querying/search support and also implements the `ActiveRecord` pattern that allows you to store active
records in vertica.

Powered by odbc_connect();

To use this extension, you have to configure the Connection class in your application configuration:

```php
return [
    //....
    'components' => [
        'vertica' => [
            'class' => 'yii\vertica\Connection',
            'dsn' => 'Driver=Vertica;Server=localhost;Database=my-database;',
            'username' => 'dbadmin',
            'password' => 'password-base',
        ],
    ]
];
```

and console.php for migrate

```php
return [
    'controllerMap' => [
        'migrate-vertica' => 'yii\vertica\controllers\MigrateVerticaController',
    ],
];
```

Requirements
------------

Performance tested on version 7.1.2

Installation
------------

The preferred way to install this extension is through [composer](http://getcomposer.org/download/).

Either add

```json
{
    "require": {
        "keygenqt/yii2-vertica": "*",
    }
}
```

to the require section of your composer.json.

Using the ActiveRecord
----------------------

The following is an example model called `Admins`:

```php
namespace app\models;

use \yii\data\ActiveDataProvider;
use \yii\vertica\ActiveRecord;

class Admins extends ActiveRecord
{
    public static function tableName() 
    {
        return 'admins';
    }

    /**
	 * @return array validation rules for model attributes.
	 */
    public function rules()
    {
        return [
            [['username', 'password_hash', 'blocked_at', 'role', 'created_at', 'updated_at'], 'safe']
        ];
    }

    ...
    
    public function search($params)
    {
        $query = Admins::find();
        $dataProvider = new ActiveDataProvider([
            'query' => $query,
        ]);

        if (!($this->load($params) && $this->validate())) {
            return $dataProvider;
        }

        $query->andFilterWhere(['like', 'username', $this->username]);
        $query->andFilterWhere(['like', 'password_hash', $this->password_hash]);
        $query->andFilterWhere(['=', 'created_at', $this->created_at]);
        $query->andFilterWhere(['=', 'updated_at', $this->updated_at]);
		
        return $dataProvider;
    }
}
```