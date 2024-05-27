<?php

declare(strict_types=1);

namespace NestboxPHP\Nestbox\Exception;

class ResultFetchException extends NestboxException
{
    function __construct(string $message = "", int $code = 0, $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}