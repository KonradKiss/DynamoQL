<?php
namespace DynamoQL\Common;
/**
 * Created by: Konrad Kiss
 * Date: 1/17/13
 * Time: 6:36 PM
 */

class Helper
{
    public static function generateSalt()
    {
        return md5("cfs" . microtime());
    }

    public static function generateHash( $secret, $salt )
    {
        return hash('sha256', $secret . $salt);
    }

    public static function generateGUID( $namespace = "" )
    {
        global $_SERVER;

        static $guid = '';
        $uid  = uniqid("", true);
        $data = $namespace;
        $data .= $_SERVER['REQUEST_TIME'];
        $data .= $_SERVER['HTTP_USER_AGENT'];
        $data .= $_SERVER['REMOTE_ADDR'];
        $data .= $_SERVER['REMOTE_PORT'];
        $hash = strtoupper(hash('ripemd128', $uid . $guid . md5($data)));

        $guid = '{' . substr($hash, 0, 8) . '-' . substr($hash, 8, 4) . '-' . substr($hash, 12, 4) . '-' . substr($hash, 16, 4) . '-' . substr($hash, 20, 12) . '}';

        return $guid;
    }

    public static function getWords( $str, $max_parameter_count = -1 )
    {
        $split = preg_split("/[\s,]*\\\"([^\\\"]+)\\\"[\s,]*|" . "[\s,]*'([^']+)'[\s,]*|" . "[\s,]+/", $str, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);

        if ( $max_parameter_count > -1 ) {
            $res = array_slice($split, 0, $max_parameter_count);
        } else {
            $res = $split;
        }

        return $res;
    }

    public static function breakupKeyValuePairs( $txt )
    {
        $split = explode(",", $txt);
        $ret   = [];
        foreach ( $split as $spl ) {
            $kv = explode("=", trim($spl));
            if ( !is_array($kv) ) {
                throw new \DynamoQL\Common\Exception\DynamoQLException("DynamoQL: Illegal key-value pair. >>> DynamoQL\\Common\\Helper::breakupKeyValuePairs('" . $txt . "')");
            }
            $ret[trim($kv[0])] = trim($kv[1], " '\"");
        }

        return $ret;
    }

    public static function breakupList( $txt, $simplify = true )
    {
        $split = explode(",", $txt);
        foreach ( $split as $sid => $spl ) {
            $split[$sid] = trim($spl);
        }

        return (count($split) > 1 || !$simplify) ? $split : (count($split) > 0 ? $split[0] : "");
    }
}
