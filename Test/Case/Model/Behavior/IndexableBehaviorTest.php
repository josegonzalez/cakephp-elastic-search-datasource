<?php
App::uses('IndexableBehavior', 'Elastic.Model/Behavior');
App::uses('ModelBehavior', 'Model');
App::uses('Model', 'Model');


class FakeSource {
	public function begin() {}
	public function commit() {}
	public function addToDocument() {}
}

/**
 * IndexableBehavior Test Case
 *
 */
class IndexableBehaviorTestCase extends CakeTestCase {
/**
 * setUp method
 *
 * @return void
 */
	public function setUp() {
		parent::setUp();
		$this->Indexable = new IndexableBehavior();
	}

/**
 * tearDown method
 *
 * @return void
 */
	public function tearDown() {
		unset($this->Indexable);
		parent::tearDown();
	}

/**
 * Test the index method and if it respects callback
 *
 * @return void
 * @author David Kullmann
 */
	public function testIndexNoCallback() {
		
		$Model = $this->getMock('Model', array('getDataSource', 'beforeSave'));
		
		$Model->alias = 'Model';
		
		$FakeSource = new FakeSource();
		
		$Model
			->expects($this->any())
			->method('getDataSource')
			->will($this->returnValue($FakeSource));
		
		$Model
			->expects($this->never())
			->method('beforeSave');
					
		$documents = array(
			array('Model' => array(
				'field' => 'value'
			)),
			array('Model' => array(
				'field' => 'value'
			))
		);
		
		$options = array();
		
		$expected = $documents;
		$result = $this->Indexable->index($Model, $documents, $options);
		$this->assertEqual($expected, $result);
	}
	
	public function testIndexWithCallback() {
		$Model = $this->getMock('Model', array('getDataSource', 'beforeSave'));
		
		$Model->alias = 'Model';
		
		$FakeSource = new FakeSource();
		
		$Model
			->expects($this->any())
			->method('getDataSource')
			->will($this->returnValue($FakeSource));
		
		$Model
			->expects($this->exactly(2))
			->method('beforeSave');
					
		$documents = array(
			array('Model' => array(
				'field' => 'value'
			)),
			array('Model' => array(
				'field' => 'value'
			))
		);
		
		$options = array('callbacks' => true);
		
		$expected = $documents;
		$result = $this->Indexable->index($Model, $documents, $options);
		$this->assertEqual($expected, $result);
	}

}
