<?php

/**
 * A simple data driver for multi type storage
 * usage:
 *     data::o(fu::config('data.driver'))->put('table_name',$data);
 *     data::o()->get('user')->limit(0, 20)->order('user_id desc')->fetch_all(true);
 * 
 * @package Fuwz
 * @subpackage data/driver/mysql
 * @version 1.2.20
 * @date 21/10/2014
 * @auther terry 
 * @email jstel@126.com
 * @license http://www.apache.org/licenses/LICENSE-2.0.html  apache-license-2.0 
 * @license http://www.fuwz.net/licenses/commercial.html  fuwz-commercial-license for licensed commercial support
 * @link http://www.fuwz.net 
 * @copyright (c) 2014 fu team
 * 
 */
class db {

    protected $config = array();
    public $db_handle_current = null;

    /**
     *
     * @return db
     */
    private static $obj = null;
    public $error;
    public $error_max_num = 20;

    public function __toString() {
        return $this->sql . "    \n" . var_export($this->key, 1);
    }

    public function __construct($config) {
        $this->config = $config;
    }

    public function connect($config) {
        if (!empty($config)) {
            $config['port'] = intval($config['port']) ? intval($config['port']) : 3306;
            $config['charset'] = intval($config['charset']) ? addslashes($config['charset']) : 'utf8';
            $dsn = 'mysql:dbname=' . $config['db'] . ';host=' . $config['host'] . ';port=' . $config['port'] . ';charset=' . $config['charset'];
            try {
                $this->db_handle_current = new PDO($dsn, $config['user'], $config['password']);
            } catch (PDOException $e) {
                echo 'Connection failed: ' . $e->getMessage();
                die();
            }
        } else {
            return FALSE;
        }
        return $this;
    }

    public function close() {
        $this->db_handle_current = null;
    }

    public function ping() {
        try {
            $this->db_handle_current->getAttribute(PDO::ATTR_SERVER_INFO);
        } catch (PDOException $e) {
            if (strpos($e->getMessage(), 'MySQL server has gone away') !== false) {
                return false;
            }
        }
        return true;
    }

    public function del($table, $where = null) {
        $this->table = $this->order = $this->limit = $this->key = $this->where = $this->sql_limit = $this->count_replace = $this->count_sql = null;
        $this->table[] = $table;
        $this->sql = 'DELETE FROM `' . $table . '`';
        if (!empty($where)) {
            $this->sql .= 'where ' . $this->build_where($where);
        }
        $sth = $this->db_handle_current->prepare($this->sql);

        $ret = $sth->execute();
        if ($ret) {
            return $ret;
        } else {
            $this->error($sth->errorCode(), $sth->errorInfo());
            return $ret;
        }
    }

    public function put($table, $data, $where = null) {
        $this->table = $this->order = $this->limit = $this->key = $this->where = $this->sql_limit = $this->count_replace = $this->count_sql = null;
        $this->table[] = $table;
        if (empty($where)) {
            return $this->insert($table, $data);
        } else {
            return $this->update($table, $data, $where);
        }
    }

    private function insert($table, $data) {
        $this->sql = ' INSERT  INTO ' . '`' . $table . '`';
        $query_k = $query_v = '';
        if (empty($data)) {
            return FALSE;
        }
        $comma = '';
        foreach ($data as $k => $v) {
            $k = addslashes($k);
            $query_k .= $comma . '`' . $k . '`';
            $query_v .= $comma . ":" . $k . "";
            $comma = ',';
        }
        $this->sql .= '(' . $query_k . ') VALUES (' . $query_v . ') ';
        $sth = $this->db_handle_current->prepare($this->sql);
        foreach ($data as $k => $v) {
            $this->key[$table][$k] = $v;
            $sth->bindValue(':' . addslashes($k), $v);
        }

        $ret = $sth->execute();
        if ($ret) {
            return $this->db_handle_current->lastInsertId();
        } else {
            $this->error($sth->errorCode(), $sth->errorInfo());
            return $ret;
        }
    }

    private function update($table, $data, $where = array()) {
        $this->sql = 'UPDATE ' . $table . ' SET ';
        if (empty($data)) {
            return FALSE;
        }
        $comma = '';
        foreach ($data as $k => $v) {
            $k = addslashes($k);
            $this->sql .= $comma . '`' . $k . "`= :" . $k;
            $comma = ',';
        }
        $where_sql = $this->build_where($where);
        if (!empty($where_sql)) {
            $this->sql .= ' where ' . $where_sql;
        }
        $sth = $this->db_handle_current->prepare($this->sql);
        foreach ($data as $k => $v) {
            $this->key[$table][$k] = $v;
            $sth->bindValue(':' . addslashes($k), $v);
        }

        $ret = $sth->execute();
        if ($ret !== FALSE) {
            return $ret;
        } else {
            $this->error($sth->errorCode(), $sth->errorInfo());
            return $ret;
        }
    }

    /**
     * mysql::o()->get(array('content'=>array('id')))->limit(100, 20);
     * mysql::o()->get('content')->limit(100, 20);
     * select * from table
     * get('table');
     * select * from table1,table2
     * get(array('table1',table2));
     * select table1.k1,table1.k2,table1.k3,table2.k6,table2.k7 from table1,table2
     * get(array('table1'=>array(k1,k2,k3),'table2'=>array(k6,k7)))
     * @param type $table
     */
    public function get($table) {
        $this->table = $this->order = $this->limit = $this->key = $this->where = $this->sql_limit = $this->count_replace = $this->count_sql = null;
        $this->table[] = $table;
        $this->sql = ' SELECT ';
        if (is_array($table)) {
            $comma = '';
            foreach ($table as $k => $v) {
                if (is_array($v)) {
                    foreach ($v as $k1 => $v1) {
                        $this->sql .= ' ' . $comma . '`' . $k . '`' . '.`' . $v1 . '`';
                        $comma = ',';
                    }
                } else {
                    $this->sql .= ' ' . $comma . '`' . $k . '`' . '.*';
                }

                $comma = ' , ';
            }
        } else {
            $this->sql .= '*';
        }
        $this->count_replace = $this->sql;
        $this->sql .= ' FROM ';
        if (is_array($table)) {
            $comma = '';
            foreach ($table as $k => $v) {
                if (is_array($v)) {
                    $this->sql .= ' ' . $comma . '`' . $k . '`';
                } else {
                    $this->sql .= ' ' . $comma . '`' . $v . '`';
                }
            }
        } else {
            $this->sql .= $table;
        }
        return $this;
    }

    public function fetch_one() {
        if (!empty($this->sql_limit)) {
            $this->sql .= $this->sql_limit;
        }
        $sth = $this->db_handle_current->prepare($this->sql);
        $sth->execute();
        $result = $sth->fetch(PDO::FETCH_ASSOC);
        if ($result === FALSE) {
            $this->error($sth->errorCode(), $sth->errorInfo());
        }
        return $result;
    }

    public function fetch_all($with_total_count = false) {

        $this->count_sql = str_replace($this->count_replace, 'select count(*) as count', $this->sql);

        if (!empty($this->sql_limit)) {
            $this->sql .= $this->sql_limit;
        }
        $sth = $this->db_handle_current->prepare($this->sql);
        $sth->execute();
        $result['data'] = $sth->fetchAll(PDO::FETCH_ASSOC);
        if ($result['data'] === FALSE) {
            $this->error($sth->errorCode(), $sth->errorInfo());
        }

        $sth = $this->db_handle_current->prepare($this->sql);
        $sth->execute();
        if ($with_total_count) {
            $sth = $this->db_handle_current->prepare($this->count_sql);
            $sth->execute();
            $result['total'] = $sth->fetchColumn();
        }

        return $result;
    }

    public function fetch_number() {
        $this->count_sql = str_replace($this->count_replace, 'select count(*) as count', $this->sql);

        $sth = $this->db_handle_current->prepare($this->count_sql);
        $sth->execute();

        return $sth->fetchColumn();
    }

    /**
     * select * from xx where a>1 and b!=2 or (c <3 xor (d=4 and e=5))
     * $where = array(
      array('a', '>', 1),
      array('and', 'b', '!=', '2'),
      array('or', array(array('c', '<', '3'),
      array('xor', array(array('d', 4), array('e', '5'))))
      )
      );
     * or
     * select * from xx where a=1 and b=2 and c=3
     * $where = array('a'=>1,'b'=>2,'c'=>3);
     * 
     * @param type $where
     * @return string
     */
    public function build_where($where) {
        if (empty($where)) {
            return '';
        }
        $where_sql = '';
        $comma = '';
        foreach ($where as $k => $v) {
            if (is_array($v) && isset($v[0])) {
                if (!isset($v[2]) && !is_array($v[1])) {
                    //array('d', 4)
                    $tmp_value = $this->raw_quote($v[1]);
                    if (empty($tmp_value)) {
                        $tmp_value = '';
                    }
                    $where_sql .= $comma . " `" . addslashes($v[0]) . "` = " . $tmp_value;
                }if (isset($v[2]) && !isset($v[3])) {
                    //array('a', '>', 1),
                    $tmp_value = $this->raw_quote($v[2]);
                    if (empty($tmp_value)) {
                        $tmp_value = '';
                    }
                    $where_sql .= $comma . " `" . addslashes($v[0]) . "` {$v[1]} " . $tmp_value;
                } elseif (isset($v[3])) {
                    //array('and', 'b', '!=', '2'),
                    $tmp_value = $this->raw_quote($v[3]);
                    if (empty($tmp_value)) {
                        $tmp_value = '';
                    }
                    $where_sql .= $comma . " " . addslashes($v[0]) . " `" . addslashes($v[1]) . "` {$v[2]} " . $tmp_value;
                } elseif (is_array($v[1])) {
                    $where_sql .= $comma . " " . addslashes($v[0]) . " (" . $this->build_where($v[1]) . ")";
                }
            } elseif (is_array($v)) {
                //array(k=>v)
                foreach ($v as $k1 => $v1) {
                    $tmp_value = $this->raw_quote($v1);
                    $where_sql .= $comma . " `" . addslashes($k1) . "` = " . $tmp_value;
                    $comma = ' and ';
                }
            } else {
                //$where = array(k=>v,kk=>vv)
                $tmp_value = $this->raw_quote($v);
                if (empty($tmp_value)) {
                    $tmp_value = '';
                }
                $where_sql .= $comma . " `" . addslashes($k) . "` = " . $tmp_value;
            }
            $comma = ' and ';
        }
        return $where_sql;
    }

    public function where($where = array()) {
        if (!empty($where)) {
            $this->sql .= ' where ' . $this->build_where($where);
        }
        return $this;
    }

    public function limit($start_num = 0, $size = 10) {
        $this->limit['start_num'] = intval($start_num);
        $this->limit['offset'] = intval($size);
        if (strpos(strtolower(' ' . trim(@$this->sql)), 'delete') == 1 or strpos(strtolower(' ' . trim(@$this->sql)), 'update') == 1) {
            $this->sql_limit = ' LIMIT ' . $size;
        } else {
            $this->sql_limit = ' LIMIT ' . $start_num . ',' . $size;
        }

        return $this;
    }

    public function group($key) {
        $this->sql .= ' group by ' . $key . ' ';
        return $this;
    }

    public function order($order) {
        $this->sql .= ' ORDER BY ' . $order;
        return $this;
    }

    public function raw_query($query) {
        return $this->db_handle_current->query();
    }

    public function raw_quote($value) {
        if (!$this->ping()) {
            $this->connect($this->config);
        }
        return $this->db_handle_current->quote($value);
    }

    public function raw_join($table, $on, $left_right_inner = '') {
        $this->table[] = $table;
        $this->sql .= ' ' . $left_right_inner . ' join ' . $table . ' on ' . $on;
        return $this;
    }

    public function raw_begin_transaction() {
        return $this->db_handle_current->beginTransaction();
    }

    public function raw_roll_back() {
        return $this->db_handle_current->rollBack();
    }

    public function raw_commit() {
        return $this->db_handle_current->commit();
    }

    public function error() {
        $arg = func_get_args();
        if (!empty($arg[0])) {
            if (isset($this->error[$this->error_max_num])) {
                array_shift($this->error);
            }
            $this->error[] = func_get_args();
        }
        return $this->error;
    }

    public function error_last() {
        return end($this->error);
    }

    /**
     * @return db
     */
    static public function o($class_name = null) {

        if (!is_a(self::$obj, get_class())) {

            $class = get_class();
            $class::$obj = new $class($class_name);
        } else {
//            $class::$obj = null;
        }
//        $p = fu::load(get_class(), $class_name);
//        if (!is_resource(self::$obj->current_db_handle)) {
//             self::$obj->connect();
//        }
        return self::$obj;
    }

}
