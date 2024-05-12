<?php

declare(strict_types=1);

namespace NestboxPHP\Nestbox\Exception;

class InvalidWhereOperator extends NestboxException
{
    function __construct(string $operator = "")
    {
        $operators = implode("\", \"", ["=", ">", "<", ">=", "<=", "<>", "!=", "BETWEEN", "LIKE", "IN"]);
        $message = ($operator)
            ? "Invalid operator ($operator) must be one of: \"$operators\""
            : "Invalid operator must be one of: \"$operators\"";

        parent::__construct($message);
    }
}
