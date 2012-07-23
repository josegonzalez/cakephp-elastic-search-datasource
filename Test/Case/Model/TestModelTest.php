<?php
class TestModeTest extends CakeTestCase {
	

	public $fixtures = array('plugin.elastic.elastic_test_model');

/**
 * Setup each test
 *
 * @return void
 * @author David Kullmann
 */
	public function setUp() {
		$this->Model = new Model(array('table' => 'test_models', 'name' => 'TestModel', 'ds' => 'test_index'));
	}

/**
 * Teardown each tests
 *
 * @return void
 * @author David Kullmann
 */
	public function tearDown() {
		unset($this->Model);
	}

/**
 * Make sure our test is setup right
 *
 * @return void
 * @author David Kullmann
 */
 	// public function testInstance() {
 	// 	$this->ds = ConnectionManager::getDataSource($this->Model->useDbConfig);
 	// 	$this->assertTrue($ds instanceof ElasticSource);
 	// 	$this->assertEquals('test_index', $this->Model->useDbConfig);
 	// }

	public function testRead() {
		$expected = array(
			'TestModel' => array(
				'id'       => 'test-model',
				'string'   => 'Analyzed for terms',
				'created'  => '2012-01-01 00:00:00',
				'modified' => '2012-02-01 00:00:00'
			)
		);
		
		$result = $this->Model->find('all');
		
		$log = ConnectionManager::getDataSource('test_index')->getLog();
		
		foreach ($log['log'] as $query) {
			echo $query['query'] . "\n\n";
		}

		debug($result);exit;
		
		$this->assertEquals($expected, $result);
	}

}
?>