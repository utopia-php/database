<?php

namespace Tests\Unit\Seeder;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Seeder\Factory;

class FactoryTest extends TestCase
{
    public function testDefineAndMake(): void
    {
        $factory = new Factory();
        $factory->define('users', function ($faker) {
            return [
                'name' => $faker->name(),
                'email' => $faker->email(),
                'age' => $faker->numberBetween(18, 65),
            ];
        });

        $doc = $factory->make('users');

        $this->assertInstanceOf(Document::class, $doc);
        $this->assertNotEmpty($doc->getAttribute('name'));
        $this->assertNotEmpty($doc->getAttribute('email'));
        $this->assertGreaterThanOrEqual(18, $doc->getAttribute('age'));
    }

    public function testMakeWithOverrides(): void
    {
        $factory = new Factory();
        $factory->define('users', function ($faker) {
            return [
                'name' => $faker->name(),
                'email' => $faker->email(),
            ];
        });

        $doc = $factory->make('users', ['name' => 'Override Name']);

        $this->assertEquals('Override Name', $doc->getAttribute('name'));
    }

    public function testMakeMany(): void
    {
        $factory = new Factory();
        $factory->define('users', function ($faker) {
            return [
                'name' => $faker->name(),
            ];
        });

        $docs = $factory->makeMany('users', 5);

        $this->assertCount(5, $docs);
        foreach ($docs as $doc) {
            $this->assertInstanceOf(Document::class, $doc);
        }
    }

    public function testUndefinedCollectionThrows(): void
    {
        $factory = new Factory();

        $this->expectException(\RuntimeException::class);
        $factory->make('nonexistent');
    }

    public function testGetFaker(): void
    {
        $factory = new Factory();
        $this->assertNotNull($factory->getFaker());
    }
}
