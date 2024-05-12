<?php

declare(strict_types=1);

namespace NestboxPHP\Nestbox\Exception;

class InvalidColumnException extends NestboxException
{
    function __construct(string $table = "", string $column = "")
    {
        if ($table) {
            $message = ($column) ? "Unknown column `$column` in table `$table`" : "Unknown column in table `$table`";
        } else {
            $message = ($column) ? "Unknown column name `$column`" : "Unknown column referenced in table";
        }

        parent::__construct($message);
    }
}
