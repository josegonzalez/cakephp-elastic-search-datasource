<?php
App::uses('Model', 'Model');

class ElasticSourceTest extends CakeTestCase {
	
	public $fixtures = array('plugin.elastic.elastic_test_model');

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
		$this->Model = ClassRegistry::init('TestModel');
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
		$expected = 'test_models';
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
		
		$Unmapped = new Model(array('table' => 'map_test', 'name' => 'MapTest', 'ds' => 'test_index'));
		
		$description = array(
			$Unmapped->alias => array(
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
		$result = $this->Es->mapModel($Unmapped, $description);
		$this->assertEquals($expected, $result);

		$expected = array(
			'id' => array('type' => 'integer', 'length' => 11),
			'string' => array('type' => 'string')
		);
		$result = $this->Es->describe($Unmapped);
		$this->assertEquals($expected, $result);
		
		$expected = true;
		$result = $this->Es->checkMapping($Unmapped);
		$this->assertEquals($expected, $result);
		
		$expected = true;
		$result = $this->Es->dropMapping($Unmapped);
		$this->assertEquals($expected, $result);
		
		$expected = false;
		$result = $this->Es->checkMapping($Unmapped);
		$this->assertEquals($expected, $result);
		
		// Also parse descriptions w/no alias
		$description = $description[$Unmapped->alias];
		
		$expected = true;
		$result = $this->Es->mapModel($Unmapped, $description);
		$this->assertEquals($expected, $result);

		$expected = array(
			'id' => array('type' => 'integer', 'length' => 11),
			'string' => array('type' => 'string')
		);
		$result = $this->Es->describe($Unmapped);
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
	
}
?>