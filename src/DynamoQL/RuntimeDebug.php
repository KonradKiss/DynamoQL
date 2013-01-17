<?php
namespace DynamoQL;
/**
 * Created by: Konrad Kiss
 * Date: 1/17/13
 * Time: 7:44 PM
 */

/**
 * A runtime HTML helper mock-up that presents debug messages in a more comprehensible manner.
 */
class RuntimeDebug
{
    /**
     * @param $r - a response array with keys: dql, insertid, cursor, rows, errno, error, errtype, type
     */
    public static function showQuery(&$r)
    {
        if ($r["errno"] == \DynamoQL\Common\Enum\Response::OK) {
            if (is_array($r["rows"]) && count($r["rows"])>0) {
                $rows = $r["rows"];
                $table_source = "<div style='padding:4px;margin:4px;'>".count($rows)." rows returned:</div><table style='color:white;border:1px solid silver;margin:2px;'>\n";
                // table header
                $table_source .= "<tr style='padding:0;margin:0;'>\n";
                $table_source .= "   <td style='padding:4px;margin:4px;border:1px solid gray;color:yellow;text-align:center;'>#</td>\n";
                foreach($rows[0] as $field => $value) {
                    $table_source .= "   <td style='padding:4px;margin:4px;border:1px solid gray;color:yellow;text-align:center;'>".$field."</td>\n";
                }
                $table_source .= "</tr>\n";
                // table rows
                foreach ($rows as $row_id => $row_data) {
                    $table_source .= "<tr style='padding:0;margin:0;'>\n";
                    $table_source .= "   <td style='padding:4px;margin:4px;border:1px solid gray;color:yellow;text-align:right'>".$row_id."</td>\n";
                    foreach($row_data as $value) {
                        $table_source .= "   <td style='padding:4px;margin:4px;border:1px solid gray;color:white;".(is_numeric($value) ? "text-align:right;" : "")."'>".$value."</td>\n";
                    }
                    $table_source .= "</tr>\n";
                }
                $table_source .= "</table>";
                RuntimeDebug::html("RESPONSE HTML OK [".$r["errno"]."]", $table_source);
            } else if (is_numeric($r["insertid"])) {
                RuntimeDebug::ok("RESPONSE INSERT OK [".$r["errno"]."] INSERTID: ".$r["insertid"]);
            } else {
                RuntimeDebug::ok("RESPONSE OK [".$r["errno"]."]");
            }
        } else {
            //$this->debugWriteErrorVerbose("RESPONSE ERROR [".$r["errno"]."]: ".$r["error"], $r);
            RuntimeDebug::error("RESPONSE ERROR [".$r["errno"]."]: ".$r["error"]." {".$r['errtype']."}");
        }
    }

    /**
     * @param        $title - a title for the debug message box
     * @param        $variable - the variable or message described in the details box
     * @param bool   $header_only - when true, only the title is displayed
     * @param string $color_title - the html color code of the title
     */
    public static function html($title, $variable, $header_only=false, $color_title="#fc6")
    {
        echo("<div style='border:2px solid white;font-family:monospace;background-color:darkslategray;color:white;margin:0px;'><div style='padding:3px;color:".$color_title.";font-weight:bold;'>".$title."</div>".($header_only?"":"<hr style='height:1px;color:#ccc;background-color:white;border:0px;' />\n".$variable)."</div>");
    }

    /**
     * @param        $title - a title for the debug message box
     * @param        $variable - the variable or message described in the details box
     * @param bool   $header_only - when true, only the title is displayed
     * @param string $color_title - the html color code of the title background
     * @param string $color_variable - the html color code of the title text
     */
    public static function message($title, $variable, $header_only=false, $color_title="#fc6", $color_variable="#cfc")
    {
        RuntimeDebug::html($title, "<pre style='padding:3px;color:".$color_variable.";'>".print_r($variable, true)."\n</pre>", $header_only, $color_title);
    }

    /**
     * @param $title - message to display
     */
    public static function ok($title)
    {
        RuntimeDebug::message($title, "", true);
    }

    /**
     * @param $title - message title to display
     * @param $variable - description to display
     */
    public static function errorVerbose($title, $variable)
    {
        RuntimeDebug::message($title, $variable, false, "#f44", "#f44");
    }

    /**
     * @param $title - error message to display
     */
    public static function error($title)
    {
        RuntimeDebug::message($title, "", true, "#f44", "#f44");
    }
 }
