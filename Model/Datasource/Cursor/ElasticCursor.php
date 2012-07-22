<?php

class ElasticCursor implements Iterator {

	protected $_iterator;

	protected $_source;

	protected $options = array();

	public function __construct(ElasticSource $source, $options) {
		$this->_source = $source;
		$this->_options = $options;
	}

	public function getIterator() {
		if (empty($this->_iterator)) {
			$method = $this->_options['method'];
			$api = $this->_options['api'];
			$index = $this->_options['type'];
			unset($this->_options['method'], $this->_options['api'], $this->_options['type']);
			$this->_iterator = new ArrayIterator(
				$this->_source->filterResults(
					$this->_source->execute($method, $index, $api, $this->_options)
				)
			);
		}
		return $this->_iterator;
	}

	public function current() {
		return $this->getIterator()->current();
	}

	public function key() {
		return $this->getIterator()->key();
	}

	public function next() {
		return $this->getIterator()->next();
	}

	public function rewind() {
		return $this->getIterator()->rewind();
	}

	public function valid() {
		return $this->getIterator()->valid();
	}
}
