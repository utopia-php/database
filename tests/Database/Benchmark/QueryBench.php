<?php

namespace Utopia\Tests\Benchmark;

use Generator;
use Utopia\Database\Query;
use Utopia\Database\QueryV1;
use PhpBench\Attributes as Bench;

class QueryBench
{
    public function provideAttributes(): Generator
    {
        yield '1 Attribute' => ['attributes' => str_repeat('"' . $this->generateRandomString() . '",', 1)];
        yield '2 Attributes' => ['attributes' => str_repeat('"' . $this->generateRandomString() . '",', 2)];
        yield '4 Attributes' => ['attributes' => str_repeat('"' . $this->generateRandomString() . '",', 4)];
        yield '8 Attributes' => ['attributes' => str_repeat('"' . $this->generateRandomString() . '",', 8)];
        yield '16 Attributes' => ['attributes' => str_repeat('"' . $this->generateRandomString() . '",', 16)];
        yield '32 Attributes' => ['attributes' => str_repeat('"' . $this->generateRandomString() . '",', 32)];
        yield '64 Attributes' => ['attributes' => str_repeat('"' . $this->generateRandomString() . '",', 64)];
        yield '128 Attributes' => ['attributes' => str_repeat('"' . $this->generateRandomString() . '",', 128)];
    }

    protected function generateRandomString(int $length = 10)
    {
        return substr(str_shuffle(str_repeat($x = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', ceil($length / strlen($x)))), 1, $length);
    }

    #[Bench\Revs(1000)]
    #[Bench\Iterations(10)]
    #[Bench\ParamProviders('provideAttributes')]
    public function benchV1(array $params)
    {
        QueryV1::parse("actors.equal({$params['attributes']})");
    }

    #[Bench\Revs(1000)]
    #[Bench\Iterations(10)]
    #[Bench\ParamProviders('provideAttributes')]
    public function benchV2(array $params)
    {
        Query::parse("equal('actors', [{$params['attributes']}])");
    }
}
