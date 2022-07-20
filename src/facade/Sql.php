<?php
/**
 * Created by PhpStorm.
 * User: xujunfeng
 * Date: 2020/9/15
 * Time: 14:11
 */

namespace think\facade;


class Sql
{
    private static $instance;
    public $sql;

    public static function table($table)
    {
        if (!(self::$instance instanceof self)) {
            self::$instance = new self();
        }
        return self::$instance->aggre($table);
    }


    function aggre($table)
    {
        $this->sql['aggregate'] = $table;
        $this->sql['pipeline'] = [];
        return $this;
    }

    function match($field, $value, $type = 0)
    {
        if ($type) {
            $value = intval($value);
        }
        $param = ['$match' => [$field => $value]];
        array_push($this->sql['pipeline'], $param);
        return $this;
    }

    function match_other($field, $regex, $value)
    {
        $param = ['$match' => [$field => ['$' . $regex => $value]]];
        array_push($this->sql['pipeline'], $param);
        return $this;
    }

    function match_and($file, $min, $max)
    {
        $param = ['$match' => [$file => ['$gte' => $min, '$lte' => $max]]];
        array_push($this->sql['pipeline'], $param);
        return $this;
    }

    function match_in($file, $data)
    {
        $param = ['$match' => [$file => ['$in' => $data]]];
        array_push($this->sql['pipeline'], $param);
        return $this;
    }

    function sort($file, $sort = 'desc')
    {
        if ($sort == "desc") {
            $sort = -1;
        } else {
            $sort = 1;
        }
        $param = ['$sort' => [$file => $sort]];
        array_push($this->sql['pipeline'], $param);
        return $this;
    }

    function unwind($field, $type = 0)
    {
        if ($type == 0) {
            $param = ['$unwind' => '$' . $field];
        } else {
            $param = ['$unwind' => ['path' => '$' . $field, 'preserveNullAndEmptyArrays' => true]];
        }
        $param = ['$unwind' => '$' . $field];
        array_push($this->sql['pipeline'], $param);
        return $this;
    }


    function field($field)
    {
        str_replace(PHP_EOL, '', $field);
        $field = explode(",", $field);
        foreach ($field as $value) {
            $field_key = "";
            $as_field = "";
            $as = explode(" as ", $value);
            if (count($as) == 2) {
                $as_field = $as[1];
            }
            $key = explode(".", $as[0]);
            if (count($key) == 3) {
                if ($as_field == "") {
                    $as_field = $key[1];
                }
                $project[$as_field] = ['$' . $key[2] => '$' . $key[0] . '.' . $key[1]];
            } elseif (count($key) == 2) {
                if ($as_field == "") {
                    $as_field = $key[1];
                }
                $project[$as_field] = '$' . $key[0] . '.' . $key[1];
            } else {
                if ($as_field == "") {
                    $as_field = $key[0];
                }
                $project[$as_field] = 1;
            }
        }
        $param = ['$project' => $project];
        array_push($this->sql['pipeline'], $param);
        return $this;
    }

    public function limit($skip, $limit)
    {
        $param = ['$skip' => $skip];
        array_push($this->sql['pipeline'], $param);
        $param = ['$limit' => $limit];
        array_push($this->sql['pipeline'], $param);
        return $this;
    }

    public function count()
    {
        $param = ['$count' => 'count'];
        array_push($this->sql['pipeline'], $param);
        return $this;
    }

    /**
     * @param $group_field
     * @param $first_field
     * @param $sum_field
     * @return $this
     */
    public function group($group_field, $first_field = "", $sum_field = "")
    {
        $project['_id'] = '$' . $group_field;
        if (empty($group_field)) {
            $project['_id'] = '';
        }
        if (!empty($first_field)) {
            $first_field = explode(",", $first_field);
            foreach ($first_field as $value) {
                $project[$value] = ['$first' => '$' . $value];
            }
        }
        if (!empty($sum_field)) {
            $project[$sum_field] = ['$sum' => 1];
        }
        $group = ['$group' => $project];
        array_push($this->sql['pipeline'], $group);
        return $this;
    }

    function lookup($table, $a_join, $b_join, $as_name, $is_unwind = 1)
    {
        $lookup = ['$lookup' => ['from' => $table, 'localField' => $a_join, 'foreignField' => $b_join, 'as' => $as_name]];
        if ($is_unwind == 1) {
            $unwind = ['$unwind' => '$' . $as_name];
        }
        array_push($this->sql['pipeline'], $lookup);
        if ($is_unwind == 1) {
            array_push($this->sql['pipeline'], $unwind);
        }
        return $this;
    }

    function lookup_let($table, $let, $pipeline, $as_name)
    {
        $lookup = ['$lookup' => ['from' => $table, 'let' => $let, 'pipeline' => $b_join, 'as' => $as_name]];
        array_push($this->sql['pipeline'], $lookup);
        return $this;
    }

    function create()
    {
        return $this->sql;
    }

    function sum($field)
    {
        $group = [
            '$group' => [
                '_id' => null,
                'total' => ['$sum' => '$' . $field]
            ]
        ];
        array_push($this->sql['pipeline'], $group);
        $sql = $this->sql;
        $data = Db::command($sql);
        if (empty($data)) {
            return 0;
        }
        return $data[0]['total'];
    }

    function find()
    {

        $limit = ['$limit' => 1];
        array_push($this->sql['pipeline'], $limit);
        $sql = $this->sql;
        $data = Db::command($sql);
        if (empty($data)) {
            return [];
        }
        return $data[0];
    }

    function select()
    {

        $sql = $this->sql;
        $data = Db::command($sql);
        return $data;
    }

}
