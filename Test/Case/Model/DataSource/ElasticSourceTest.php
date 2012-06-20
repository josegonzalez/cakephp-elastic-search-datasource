<?php
App::uses('Model', 'Model');

class ElasticSourceTestModel extends Model {
	
	public $useDbConfig = 'test_index';
	
}
class ElasticSourceTest extends CakeTestCase {
	
	public $fixtures = array('elastic.elastic_source_test');

/**
 * Setup each test
 *
 * @return void
 * @author David Kullmann
 */
	public function setUp() {
		$this->Es = ConnectionManager::getDataSource('test_index');
		if (!($this->Es instanceof ElasticSource)) {
			$this->markTestSkipped('Unable to load elastic_test datasource for ElasticSource');
		}
		$this->Model = ClassRegistry::init('ElasticSourceTestModel');
	}

/**
 * Teardown each tests
 *
 * @return void
 * @author David Kullmann
 */
	public function tearDown() {
		unset($this->Model);
		ClassRegistry::flush();
	}

/**
 * undocumented function
 *
 * @return void
 * @author David Kullmann
 * @expectedException MissingIndexException
 */
	public function testMissingIndexException() {
		$original_index = $this->Es->config['index'];
		
		$this->Es->config['index'] = 'a_new_fake_index';
		try {
			$result = $this->Es->describe($this->Model);
		} catch (MissingIndexException $e) {
			$this->Es->config['index'] = $original_index;
			throw $e;
		}
	}

/**
 * Test the getType method
 *
 * @return void
 * @author David Kullmann
 */
	public function testGetType() {
		$expected = 'elastic_source_test_models';
		$result = $this->Es->getType($this->Model);
		
		$this->assertEquals($expected, $result);
		
		$expected = 'custom_type';
		$this->Model->useType = 'custom_type';
		
		$result = $this->Es->getType($this->Model);
		
		$this->assertEquals($expected, $result);
	}

/**
 * test creating and dropping a mapping
 *
 * @return void
 * @author David Kullmann
 */
	public function testMapping() {
		$description = array(
			$this->Model->alias => array(
				'id' => array(
					'key' => 'primary',
					'length' => 11,
					'type' => 'integer'
				),
				'string' => array(
					'type' => 'string',
					'length' => 255,
					'null' => false,
					'default' => null
				)
			)
		);
		
		$expected = true;
		$result = $this->Es->mapModel($this->Model, $description);
		$this->assertEquals($expected, $result);

		$expected = array(
			'id' => array('type' => 'integer', 'length' => 11),
			'string' => array('type' => 'string')
		);
		$result = $this->Es->describe($this->Model);
		$this->assertEquals($expected, $result);
		
		$expected = true;
		$result = $this->Es->checkMapping($this->Model);
		$this->assertEquals($expected, $result);
		
		$expected = true;
		$result = $this->Es->dropMapping($this->Model);
		$this->assertEquals($expected, $result);
		
		$expected = false;
		$result = $this->Es->checkMapping($this->Model);
		$this->assertEquals($expected, $result);
		
		// Also parse descriptions w/no alias
		$description = $description[$this->Model->alias];
		
		$expected = true;
		$result = $this->Es->mapModel($this->Model, $description);
		$this->assertEquals($expected, $result);

		$expected = array(
			'id' => array('type' => 'integer', 'length' => 11),
			'string' => array('type' => 'string')
		);
		$result = $this->Es->describe($this->Model);
		$this->assertEquals($expected, $result);
	}

/**
 * Test parsing mappings for multiple types, multiple models, etc
 *
 * @return void
 * @author David Kullmann
 */
	public function testParseMapping() {
		
		$mapping = array('index' => array(
			'type' => array(
				'properties' => array(
					'Alias' => array(
						'properties' => array(
							'id' => array('type' => 'integer'),
							'string' => array('type' => 'string')
						)
					)
				)
			)
		));
		
		$expected = array('Alias' => array(
			'id' => array('type' => 'integer'),
			'string' => array('type' => 'string')
		));
		$result = $this->Es->parseMapping($mapping);
		$this->assertEquals($expected, $result);
		
		$mapping = array('index' => array(
			'type' => array(
				'properties' => array(
					'Alias' => array(
						'properties' => array(
							'id' => array('type' => 'integer'),
							'string' => array('type' => 'string')
						)
					),
					'RelatedModel' => array(
						'properties' => array(
							'id' => array('type' => 'integer'),
							'float' => array('type' => 'float')
						)
					)
				)
			)
		));
		
		$expected = array(
			'Alias' => array(
				'id' => array('type' => 'integer'),
				'string' => array('type' => 'string')
			),
			'RelatedModel' => array(
				'id' => array('type' => 'integer'),
				'float' => array('type' => 'float')
			)
		);
		$result = $this->Es->parseMapping($mapping);
		$this->assertEquals($expected, $result);
		
		$expected = array('type');
		$result = $this->Es->parseMapping($mapping, true);
		$this->assertEquals($expected, $result);
		
		$mapping = array('index' => array(
			'type' => array(
				'properties' => array(
					'Alias' => array(
						'properties' => array(
							'id' => array('type' => 'integer'),
							'string' => array('type' => 'string')
						)
					)
				)
			),
			'type2' => array(
				'properties' => array(
					'AnotherModel' => array(
						'properties' => array(
							'id' => array('type' => 'integer'),
							'string' => array('type' => 'string')
						)
					)
				)
			)
		));
		
		$expected = array(
			'Alias' => array(
				'id' => array('type' => 'integer'),
				'string' => array('type' => 'string')
			),
			'AnotherModel' => array(
				'id' => array('type' => 'integer'),
				'string' => array('type' => 'string')
			)
		);
		$result = $this->Es->parseMapping($mapping);
		$this->assertEquals($expected, $result);
		
		$expected = array('type', 'type2');
		$result = $this->Es->parseMapping($mapping, true);
		$this->assertEquals($expected, $result);	
	}

/**
 * Test the update function
 *
 * @return void
 * @author David Kullmann
 */
	public function testUpdate() {
		
		$description = array(
			$this->Model->alias => array(
				'id' => array(
					'key' => 'primary',
					'length' => 11,
					'type' => 'integer'
				),
				'string' => array(
					'type' => 'string',
					'length' => 255,
					'null' => false,
					'default' => null
				)
			)
		);
		
		$expected = true;
		$result = $this->Es->mapModel($this->Model, $description);
		$this->assertEquals($expected, $result);
		

		$data = array(
			'id' => 123,
			'string' => 'test'
		);
		
		$expected = array($this->Model->alias => $data);
		$result = $this->Model->save($data);

		$this->assertEquals($expected, $result);
		
		$expected = true;
		$result = $this->Model->delete($data['id']);
		$this->assertEquals($expected, $result);
		
	}
	
}
?>