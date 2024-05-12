<?php

declare(strict_types=1);

namespace NestboxPHP\Nestbox\Exception;

class MismatchedColumnNamesException extends NestboxException
{
    function __construct(array $array1 = [], array $array2 = [])
    {
        if ($array1 and $array2) {
            if (is_array(current($array1))) $array1 = current($array1);
            if (is_array(current($array2))) $array2 = current($array2);
            $array1 = implode(", ", $array1);
            $array2 = implode(", ", $array2);
        }

        $message = ($array1 and $array2) ? "Mismatched column names: ($array1) != ($array2)" : "Mismatched column names";

        parent::__construct($message);
    }
}