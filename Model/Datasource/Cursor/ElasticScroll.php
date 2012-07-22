<?php

App::uses('ElasticCursor', 'ElasticSearch.Model/Datasource/Cursor');

class ElasticScroll extends AppendIterator {

	protected $_source;

	protected $_total = 0;

	protected $_pageSize = 0;

	protected $_current = 0;

	protected $_totalPages = 1;

	protected $_scrollId;

	protected $_options;

	public function __construct(ElasticSource $source, $options = array()) {
		$this->_source = $source;
		$this->_total = $options['total'];
		$this->_pageSize = $options['limit'];
		$this->_scrollId = $options['scrollId'];
		$this->_totalPages = ceil($this->_total / $this->_pageSize);

		unset($options['limit'], $options['scrollId'], $options['total']);
		$this->_options = $options;
		parent::__construct();
	}

	public function valid() {
		$valid = parent::valid();
		if (!$valid && $this->_current < $this->_totalPages) {
			$from = $this->_current * $this->_pageSize;
			
			$query = array(
				'scroll_id' => $this->_scrollId,
				'from' => $from,
				'size' => $this->_pageSize
			);

			$this->append(new ElasticCursor($this->_source, compact('query') + $this->_options));
			$this->_current++;
			$valid = true;
		}
		return $valid;
	}

}
