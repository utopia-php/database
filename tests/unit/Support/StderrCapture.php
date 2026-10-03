<?php

namespace Tests\Unit\Support;

use ArrayObject;
use php_user_filter;

final class StderrCapture extends php_user_filter
{
    private const string FILTER = 'tests.stderr.capture';

    /**
     * Run the callback with everything it writes to STDERR recorded instead of printed.
     *
     * @param  callable(): void  $callback
     */
    public static function during(callable $callback): string
    {
        if (! \in_array(self::FILTER, \stream_get_filters(), true)) {
            \stream_filter_register(self::FILTER, self::class);
        }

        /** @var ArrayObject<int, string> $chunks */
        $chunks = new ArrayObject();
        $filter = \stream_filter_append(\STDERR, self::FILTER, \STREAM_FILTER_WRITE, $chunks);

        try {
            $callback();
        } finally {
            if ($filter !== false) {
                \stream_filter_remove($filter);
            }
        }

        return \implode('', $chunks->getArrayCopy());
    }

    /**
     * @param  resource  $in
     * @param  resource  $out
     * @param  int  $consumed
     */
    public function filter($in, $out, &$consumed, bool $closing): int
    {
        while ($bucket = \stream_bucket_make_writeable($in)) {
            if ($this->params instanceof ArrayObject) {
                $this->params->append($bucket->data);
            }
            $consumed += $bucket->datalen;
        }

        return \PSFS_PASS_ON;
    }
}
