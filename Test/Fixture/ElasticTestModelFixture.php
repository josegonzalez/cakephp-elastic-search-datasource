<?php
App::uses('ElasticFixture', 'Elastic.Test/Fixture');
/**
 * ElasticTestModelFixture
 *
 * @package default
 * @author David Kullmann
 */
class ElasticTestModelFixture extends ElasticFixture {

/**
 * Fixture model name
 *
 * @var string
 */
	public $name = 'TestModel';


/**
 * 'init' used to set dynamic variables like datetime
 *
 * @return void
 * @author David Kullmann
 */
	public function init() {
		# Access $this->records and Set dynamic date/time here
		parent::init();
	}

/**
 * Fixture fields
 *
 * @var array
 */
	public $fields = array(
		// Identifier
		'id' => array('type' => 'string', 'null' => false, 'default' => NULL, 'length' => 36, 'key' => 'primary', 'index' => 'not_analyzed'),
		
		// String
		'string'     => array('type' => 'string', 'null' => false, 'default' => NULL, 'length' => 10),
		
		// Datetime
		'created'  => array('type' => 'datetime', 'null' => false, 'default' => NULL),
		'modified' => array('type' => 'datetime', 'null' => false, 'default' => NULL)
	);

/**
 * Fixture records
 *
 * @var array
 */
	public $records = array(
		array(
			'id'       => 'test-model',
			'string'   => 'Analyzed for terms',
			'created'  => '2012-01-01 00:00:00',
			'modified' => '2012-02-01 00:00:00'
		),
	);
}
?>