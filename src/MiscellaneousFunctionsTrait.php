<?php

namespace NestboxPHP\Nestbox;

trait MiscellaneousFunctionsTrait
{
    /**
     * Generate HTML code for a two-dimensional array
     *
     * @param string $table
     * @param string $tableClass
     * @param array $columnClass
     * @return string
     */
    public static function html_table(array $table, string $tableClass = "", array $columnClass = []): string
    {
        // table start
        $code = "";
        $code .= "<table class='{$tableClass}'>";

        // add headers
        $hdrs = "";
        foreach ($table[0] as $col => $data) {
            $class = (array_key_exists($col, $columnClass)) ? "class='{$columnClass[$col]}'" : "";
            $hdrs .= "<th {$class}>{$col}</th>";
        }
        $code .= "<tr>{$hdrs}</tr>";

        // add data
        foreach ($table as $tblRow) {
            $row = "";
            foreach ($tblRow as $col => $val) {
                $class = (array_key_exists($col, $columnClass)) ? "class='{$columnClass[$col]}'" : "";
                $row .= "<td {$class}>{$val}</td>";
            }
            $code .= "<tr>{$row}</tr>";
        }

        // table end
        $code .= "</table>";
        return $code;
    }
}