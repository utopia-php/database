<?php

namespace Utopia\Database;

use Utopia\Database\Arithmetic\Rounding;

/**
 * How an engine stored a fractional result in an integer column on database 7.4.1, which handed fractional
 * amounts and limits to the engine: the rounding of a counter change, of an operator, of a division by a whole
 * number and of a power; whether it refused a fractional counter change; the operators it computed exactly; the
 * operators whose fractional amount it read as an integer, and so refused, unless a limit came with it; and
 * whether it cut a fractional modulus toward zero before taking the remainder.
 */
final readonly class Arithmetic
{
    /**
     * @param  list<OperatorType>  $exactOperators
     * @param  list<OperatorType>  $refusedWithoutLimit
     */
    public function __construct(
        public Rounding $counter,
        public Rounding $operators,
        public Rounding $division,
        public Rounding $power,
        public bool $refusesCounterFractions = false,
        public array $exactOperators = [],
        public array $refusedWithoutLimit = [],
        public bool $truncatesModulus = false,
    ) {
    }

    /**
     * The rounding of an operator's result, by its method and whether its operand is a whole number.
     */
    public function rounding(OperatorType $method, bool $whole): Rounding
    {
        return match (true) {
            $method === OperatorType::Power => $this->power,
            $method === OperatorType::Divide && $whole => $this->division,
            default => $this->operators,
        };
    }
}
