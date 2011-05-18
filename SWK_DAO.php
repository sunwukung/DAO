<?php

/**
 *
 * Simple Class to wrap common mysql queries
 *
 */
class SWK_Dao {

    protected $_identifier;
    protected $_table;
    protected $_db;
    protected $_mode = PDO::FETCH_ASSOC;

    public function __construct($db, $table = null, $id = null) {

        $this->_init($db, $table, $id);
    }

    /**
     * set a new db connection
     *
     * @param <type> $db
     * @return DV_Dao_Db
     */
    public function setConnection(PDO $db) {
        $this->_db = $db;
        $this->_db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        return $this;
    }

    /**
     * set error handling configuration
     *
     * @param string $mode must be valid PDO error handling setting
     */
    public function setErrorHandling($mode) {
        $this->_db->setAttribute(PDO::ATTR_ERRMODE, $mode);
    }

    /**
     * retrieve a fresh uuid from the current database connections
     *
     * @return string
     */
    public function getUID() {
        $result = $this->executeSql('SELECT UUID() as uuid');
        return $result[0]['uuid'];
    }

    /**
     * initialises the class
     *
     * @param PDO $db
     * @param string $table
     * @param string $id
     *
     */
    protected function _init(PDO $db, $table, $id) {
        $this->_db = $db;
        $this->_table = $this->_initialiseTable($table);
        $this->_identifier = (!empty($id)) ? $id : 'id_' . $this->_table;

        $this->_db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    }

    /**
     * initialises the table name
     *
     * detects if a table name has been specified
     *
     *
     * @param string $table
     * @return string
     */
    protected function _initialiseTable($table) {
        if (!empty($table)) {
            return $table;
        } else {
            return strtolower(substr(strrchr(get_class($this), '_'), 1));
        }
    }

    /**
     * sets the table name on the class
     *
     * @param string $table_name
     * @return mixed
     */
    public function setTable($table) {
        $this->_table = $table;
        $this->_identifier = 'id_' . $this->_table;
        return $this;
    }

    /**
     * returns name of table being queried
     *
     * @return string
     */
    public function getTable() {
        return $this->_table;
    }

    /**
     * returns name of column used to identify records
     *
     * @return string
     */
    public function getIdentifier() {
        return $this->_identifier;
    }

    /**
     * sets the name of the column used to identify records
     *
     * allows for chaining
     *
     * @param string $table_name
     * @return mixed
     */
    public function setIdentifier($id) {
        $this->_identifier = $id;
        return $this;
    }

    /**
     * set the fetch mode for the query
     *
     * allows for chaining
     *
     * @param string $mode
     * @return DV_Dao_Db
     */
    protected function setQueryMode($mode) {
        if (!empty($mode)) {
            $this->_mode = $mode;
        }
        return $this;
    }

    /**
     * retrieve all columns and rows from the currently defined table
     */
    public function select($table = null) {
        if (!empty($table)) {
            $this->_table = $table;
        }

        $sql = 'SELECT * FROM ' . $this->_table;
        return $this->executeSql($sql);
    }

    /**
     * returns unique record
     *
     * will return false if multiple records returned
     * you must use a unique identifier in conjunction with this method
     * if you set the identifier to a non-unique column it will return multiple
     * results - causing the method to report a failed routine
     *
     * @param string|int $id
     * @return DV_Dao_Db
     */
    public function selectId($id) {
        $sql = 'SELECT * FROM ' . $this->_table . ' WHERE ' . $this->_identifier . ' = :value';
        $stmt = $this->_db->prepare($sql);
        $stmt->bindParam(':value', $id, PDO::PARAM_STR);
        return $this->executeStmt($stmt);
    }

    /**
     * selects record sets according to columns and values specified in parameters
     *
     * @param string $params search criteria
     * @return DV_Dao_Db
     */
    public function selectWhere($params) {
        $sql = 'SELECT * FROM ' . $this->_table;
        $sql .= $this->buildWhere($params);
        $stmt = $this->_db->prepare($sql);
        $stmt = $this->bindSqlParams($stmt, $params, 'w');
        return $this->executeStmt($stmt);
    }

    /**
     * Utility method to construct extended WHERE clause from array
     *
     * this method generates a sql string from an associative array
     * each of the values is automatically replaced with a placeholder
     * query is supplied as colum:value or column:array(value1,value2,...)
     * values have to be supplied as ints or strings
     *
     * @param array $params associative array
     * @todo may require more freedom of possible column value datatypes
     */
    protected function buildWhere($params) {
        $where = ' WHERE ';
        $i = 0;
        foreach ($params as $column => $value) {
            if ($i > 0) {
                $where .= ' AND ';
            }

            $where .= $column;

            //process arrays of values
            if (is_array($value)) {
                //reject associative arrays
                $j = 1;
                $where .= ' IN (';
                foreach ($value as $v) {
                    //require strings|ints
                    $parameter = ':w_' . $column . '_' . $j;
                    if ($j > 1) {
                        $where .= ' , ';
                    }
                    $where .= $parameter;
                    $j++;
                }
                $where .= ')';
            } else {
                $parameter = ':w_' . $column;
                $where .= '=' . $parameter;
            }
            $i++;
        }
        return $where;
    }

    /**
     * binds placeholder parameters generated by DV_Dao_Db::buildWhere to appropriate values
     *
     * if you are calling bindSqlParams in conjunction with a buildWhere or buildValues function
     * you must specify which parameter set you are binding with the prefix
     *
     * @param PDOStatement $stmt
     * @param array $params associative array
     * @param string $prefix
     * @return PDOStatement
     */
    protected function bindSqlParams(PDOStatement $stmt, $params, $prefix = null) {
        $prefix = empty($prefix) ? $prefix : $prefix . '_';
        foreach ($params as $column => $value) {
//process multiple values
            if (is_array($value)) {
//reject associative arrays
                $j = 1;
                foreach ($value as $v) {
//require strings|ints
                    $parameterName = ':' . $prefix . $column . '_' . $j;
                    $stmt->bindValue($parameterName, $v, PDO::PARAM_STR);
                    $j++;
                }
            } else {
//reject non-string values
                $parameterName = ':' . $prefix . $column;
                $stmt->bindValue($parameterName, $value, PDO::PARAM_STR);
            }
        }
        return $stmt;
    }

    /**
     * provides flexible query generation interface to allow for more complex select statements
     *
     * @param array $params
     * @return DV_Dao_Db
     */
    public function selectFilter($params) {
        $where = $in = $order = $vector = $limit = $like = '';
        if (isset($params['columns'])) {
            $columns = '';
            if (is_array($params['columns'])) {
                $columns .= implode(',', $params['columns']);
            } else {
//single string provided
                $columns .= $params['columns'];
            }
        } else {
            $columns = '*';
        }

        if (isset($params['where'])) {
            $where = $this->buildWhere($params['where']);
        }

//LIKE
        if (isset($params['like'])) {
            $like = isset($params['where']) ? ' AND ' : ' WHERE ';

            $i = 0;
            foreach ($params['like'] as $k => $v) {
                if ($i > 0) {
                    $like .= ' AND ';
                }
                $like .= $k . ' LIKE \'' . strtolower($v) . '%\'';
                $i++;
            }
        }

//ORDER
        if (isset($params['order'])) {
            $order = ' ORDER BY ' . $params['order'];

//VECTOR (only processed if ORDER parameter present
            if (isset($params['vector'])) {
                $order.= ' ' . strtoupper($params['vector']);
            }
        }

//LIMIT
        if (isset($params['limit'])) {
            $limit = ' LIMIT ' . $params['limit'];
        }

        $sql = 'SELECT ' . $columns;
        $sql .= ' FROM ' . $this->_table;
        $sql .= $where;
        $sql .= $like;
        $sql .= $order;
        $sql .= $vector;
        $sql .= $limit;

        $stmt = $this->_db->prepare($sql);
        if ($where != '') {
            $stmt = $this->bindSqlParams($stmt, $params['where']);
        }
        return $this->executeStmt($stmt);
    }

    /**
     * provides means to run like query with multiple criteria and ordered by likeness
     *
     * @param array $params indexed array of options
     * @param array $params['columns']: columns to retrieve
     * @param array $params['like']: array(indexed or associative, indexed for multiple conditions on same column) of like conditions to be met
     * @param str $params['type']: and/or - specifies the LIKE mode (optional, defaults to AND)
     * @param str $params['order']: specifies a column to order by (optional, defaults to CASE/relevance)
     * @param str $params['sort']: specifies a sort direction (optional, defaults to ASC)
     */
    public function selectLike($params) {
        $sql = 'SELECT ';
        $case = '';

//COLUMNS
        if (isset($params['columns'])) {
//ensure assoc
            $i = 0;
            foreach ($params['columns'] as $c) {
                if ($i > 0) {
                    $sql .= ',';
                }
                $sql .= $c;
                $i++;
            }
        } else {
            $sql .= '*';
        }

        $sql .= ' FROM ' . $this->_table . ' WHERE ';

        if (isset($params['sort'])) {
//ensure string
            $sort = strtoupper($params['sort']);
        } else {
            $sort = 'ASC';
        }

//CONDITION
        $join = false;

        if (isset($params['type'])) {
//ensure string
            $sqljoin = strtoupper($params['type']);
        } else {
            $sqljoin = 'AND';
        }

        foreach ($params['like'] as $k => $v) {
            if (!is_array($v)) {
                if ($join == true) {
                    $sql .= $sqljoin; //prepend each cycle with AND / OR
                    $case .= ' + '; //prepend each cycle with +
                }
                $v = $this->_db->quote('%' . $v . '%'); //quote value
                $sql .= " `$k` LIKE $v ";
                $case .= "(CASE WHEN `$k` LIKE $v THEN 1 ELSE 0 END)";
                $join = true;
            } else {
                foreach ($v as $p) {
                    if ($join == true) {
                        $sql .= $sqljoin; //prepend each cycle with AND / OR
                        $case .= ' + '; //prepend each cycle with +
                    }
                    $p = $this->_db->quote('%' . $p . '%'); //quote value
                    $sql .= " $k LIKE $p ";
                    $case .= "(CASE WHEN `$k` LIKE $p THEN 1 ELSE 0 END)";
                    $join = true;
                }
            }
        }

//ORDER
//ensure order is a string
        if (isset($params['order'])) {
            $order_constraint = $params['order'];
        } else {
            $order_constraint = $case;
        }
        $order_state = ' ORDER BY (' . $order_constraint . ') ' . $sort;
        $sql .= $order_state; //append ordering clause
//EXECUTE
        $stmt = $this->_db->prepare($sql);
        return $this->executeStmt($stmt);
    }

    /**
     * deletes a record based on identifier
     * 
     * @param string $identifier
     * @return DV_Dao_Db
     */
    public function deleteId($identifier) {
        $sql = 'DELETE FROM ' . $this->_table . ' WHERE ' . $this->_identifier . '=:id';
        $stmt = $this->_db->prepare($sql);
        $stmt->bindValue(':id', $identifier, PDO::PARAM_INT);
        $this->executeStmt($stmt, 'delete');
        return $this;
    }

    /**
     * delete
     *
     * @param <type> $identifier
     * @return DV_Dao_Db
     */
    public function deleteWhere($params) {

        $sql = 'DELETE FROM ' . $this->_table;
        $sql .= $this->buildWhere($params);
        $stmt = $this->_db->prepare($sql);
        $stmt = $this->bindSqlParams($stmt, $params, 'w');
        return $this->executeStmt($stmt, FALSE);
    }

    /**
     * deletes a range of records using the identifying column against a range of values supplied as an array
     *
     * @param array $params
     * @return DV_Dao_Db
     */
    public function deleteRange(array $params) {
        $sql = 'DELETE FROM ' . $this->_table . ' WHERE ' . $this->_identifier . ' IN (' . implode(',', $params) . ')';
        $stmt = $this->_db->prepare($sql);
        $this->executeStmt($stmt, 'delete');
        return $this;
    }

    /**
     * insert values into the target table
     *
     * @param array $params
     * @return DV_Dao_Db
     */
    public function insert($params) {
        $placeholders = array();
        foreach ($params as $key => $value) {
            $placeholders[] = ':' . $key;
        }
        $sql = 'INSERT INTO ' . $this->_table . '(' . implode(',', array_keys($params)) . ') VALUES(' . implode(',', $placeholders) . ')';
        $stmt = $this->_db->prepare($sql);
        $stmt = $this->bindSqlParams($stmt, $params);
        $this->executeStmt($stmt, false);
        return $this;
    }

    /**
     * gets all identifiers from table
     *
     * @return DV_Dao_Db
     */
    public function rangeID() {
        $sql = 'SELECT ' . $this->_identifier . ' FROM ' . $this->_table;
        return $this->_db->exec($sql);
    }

    /**
     * update the given row in the database
     * 
     * @param array $values
     * @param array $where
     * @return DV_Dao_Db
     *
     * 
     */
    public function update($values, $where) {

        $sql = 'UPDATE ' . $this->_table . ' SET ';
        $sql .= $this->buildValues($values);
        $sql .= $this->buildWhere($where);
        $stmt = $this->_db->prepare($sql);
        $stmt = $this->bindSqlParams($stmt, $where, 'w');
        $stmt = $this->bindSqlParams($stmt, $values, 'v');
        $this->executeStmt($stmt, false);
        return $this;
    }

    /**
     * generates the value component of a sql string, replacing column names with parameter binding names
     *
     * @param array $params
     * @return string
     */
    protected function buildValues(array $params) {
        $pairs = array();
        foreach ($params as $key => $value) {
            $pairs[] = $key . ' = :v_' . $key;
        }
        return implode(',', $pairs);
    }

    /**
     * runs a raw sql query string
     *
     * can pass in optional array of name:value pairs in an associative array
     * to bind to placeholders in the string
     * string placeholders must take the form :<column name>
     * and match the indices in the parameter array
     * i.e.
     * $Dao->query('INSERT INTO users (firstname,lastname,email,password) VALUES (:firstname,:lastname,:email,:password)',array(
     *      'firstname'=>'some',
     *      'lastname'=>'body',
     *      'email'=>'somebody@something.org',
     *      'password'=>'password'
     *  ));
     *
     * @param string $sql
     * @return DV_Dao_Db
     */
    public function query($sql, $params = null) {
        $stmt = $this->_db->prepare($sql);
        if (!empty($params)) {
            $this->bindSqlParams($stmt, $params);
        }
//detect query type to determin appropriate feedback string
        $scanstr = strtoupper($sql);
        switch ($scanstr) {
            case (strpos($scanstr, 'INSERT')):
                $operation = 'insert';
                break;
            case (strpos($scanstr, 'UPDATE')):
                $operation = 'update';
                break;
            case (strpos($scanstr, 'DELETE')):
                $operation = 'delete';
                break;
            default:
                $operation = 'select';
        }
        $this->executeStmt($stmt, $operation);
        return $this;
    }

    /**
     * provides means to execute arbitrary SQL strings
     *
     * @param <type> $sql
     */
    public function executeSql($sql, $fetch = true) {

        $this->sql = $sql;

        //$l = fopen('./log.log', 'a+');
        //fwrite($l, 'execute called - ' . $this->sql);

        $stmt = $this->_db->query($sql);
        if ($fetch) {
            return $stmt->fetchAll($this->_mode);
        }
    }

    /**
     * executes statements in try/catch clause
     *
     * @param PDOStatement $stmt
     * @return boolean
     */
    protected function executeStmt(PDOStatement $stmt, $fetch = true) {
        $this->sql = $stmt->queryString;
        $stmt->execute();
        if ($fetch === TRUE) {
            return $stmt->fetchAll($this->_mode);
        }
    }

}

