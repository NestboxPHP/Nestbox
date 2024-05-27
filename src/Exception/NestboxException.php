<?php

declare(strict_types=1);

namespace NestboxPHP\Nestbox\Exception;

use RuntimeException;
use NestboxPHP\Nestbox\Nestbox;

class NestboxException extends RuntimeException
{
    function __construct(string $message = "", int $code = 0, $previous = null)
    {
        parent::__construct($message, $code, $previous);

        $this->log_exception();
    }

    protected function log_exception(): void
    {
        $message = $this->getMessage();
        $request = $_SERVER["REQUEST_URI"];
        $details = json_encode($this->getTrace());

        $nestbox = new Nestbox();
        $nestbox->log_error($message, $request, $details);
    }
}

