<?php
/**
 * This view is used by app/command/MigrateVerticaController.php
 * The following variables are available in this view:
 */
/* @var $className string the new migration class name */

echo "<?php\n";
?>

use yii\vertica\Schema;
use yii\vertica\Migration;

class <?= $className ?> extends Migration
{
    public function up()
    {

    }

    public function down()
    {
        echo "<?= $className ?> cannot be reverted.\n";
        return true;
    }
}
