<?php

namespace Utopia\Database\Loading;

enum LoadingStrategy
{
    case Eager;
    case Lazy;
    case None;
}
