<?php

namespace Tests\E2E\Adapter\Support;

use Utopia\Database\Document;

class Post extends Document
{
    public function getTitle(): string
    {
        /** @var string $title */
        $title = $this->getAttribute('title', '');

        return $title;
    }

    public function getContent(): string
    {
        /** @var string $content */
        $content = $this->getAttribute('content', '');

        return $content;
    }
}
