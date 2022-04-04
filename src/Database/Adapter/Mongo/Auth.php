<?php

namespace Utopia\Database\Adapter\Mongo;

class Auth
{
  private $authcid;
  private $secret;
  private $authzid;
  private $service;
  private $hostname;
  private $gs2Header;
  private $cnonce;
  private $firstMessageBare;
  private $saltedPassword;
  private $authMessage;
  private $nonce;

  public function __construct(array $options)
  {
    $this->authcid = isset($options['authcid']) ? $options['authcid'] : null;
    $this->secret = isset($options['secret']) ? $options['secret'] : null;
    $this->authzid = isset($options['authzid']) ? $options['authzid'] : null;
    $this->service = isset($options['service']) ? $options['service'] : null;

    $this->nonce = base64_encode(random_bytes(32));
  }

  public function createResponse($challenge = null)
  {
      $authcid = $this->formatName($this->authcid);
      if (empty($authcid)) {
          return false;
      }

      $authzid = $this->authzid;

      if (!empty($authzid)) {
          $authzid = $this->formatName($authzid);
      }

      if (empty($challenge)) {
          return $this->generateInitialResponse($authcid, $authzid);
      } else {
          return $this->generateResponse($challenge, $this->secret);
      }
  }

  /**
   * Prepare a name for inclusion in a SCRAM response.
   *
   * @param string $username a name to be prepared.
   * @return string the reformated name.
   */
  private function formatName($username)
  {
      return str_replace(array('=', ','), array('=3D', '=2C'), $username);
  }

  /**
   * Generate the initial response which can be either sent directly in the first message or as a response to an empty
   * server challenge.
   *
   * @param string $authcid Prepared authentication identity.
   * @param string $authzid Prepared authorization identity.
   * @return string The SCRAM response to send.
   */
  private function generateInitialResponse($authcid, $authzid)
  {
      $gs2CbindFlag   = 'n,';
      $this->gs2Header = $gs2CbindFlag . (!empty($authzid) ? 'a=' . $authzid : '') . ',';

      // I must generate a client nonce and "save" it for later comparison on second response.
      $this->cnonce = $this->generateCnonce();

      $this->firstMessageBare = 'n=' . $authcid . ',r=' . $this->cnonce;
      return $this->gs2Header . $this->firstMessageBare;
  }

  /**
   * Parses and verifies a non-empty SCRAM challenge.
   *
   * @param  string $challenge The SCRAM challenge
   * @return string|false      The response to send; false in case of wrong challenge or if an initial response has not
   * been generated first.
   */
  private function generateResponse($challenge, $password)
  {
      $matches = array();

      $serverMessageRegexp = "#^r=([\x21-\x2B\x2D-\x7E/]+)"
          . ",s=((?:[A-Za-z0-9/+]{4})*(?:[A-Za-z0-9/+]{3}=|[A-Za-z0-9/+]{2}==)?)"
          . ",i=([0-9]*)(,[A-Za-z]=[^,])*$#";

      if (!isset($this->cnonce, $this->gs2Header) || !preg_match($serverMessageRegexp, $challenge, $matches)) {
          return false;
      }
      $nonce = $matches[1];
      $salt  = base64_decode($matches[2]);

      if (!$salt) {
          return false;
      }
      $i = intval($matches[3]);

      $cnonce = substr($nonce, 0, strlen($this->cnonce));
      if ($cnonce !== $this->cnonce) {
          return false;
      }

      $channelBinding       = 'c=' . base64_encode($this->gs2Header);
      $finalMessage         = $channelBinding . ',r=' . $nonce;
      $saltedPassword       = $this->hi($password, $salt, $i);
      $this->saltedPassword = $saltedPassword;
      $clientKey            = $this->hmac($saltedPassword, "Client Key", true);
      $storedKey            = $this->hash($clientKey, true);
      $authMessage          = $this->firstMessageBare . ',' . $challenge . ',' . $finalMessage;
      $this->authMessage    = $authMessage;
      $clientSignature      = $this->hmac($storedKey, $authMessage, true);
      $clientProof          = $clientKey ^ $clientSignature;
      $proof                = ',p=' . base64_encode($clientProof);

      return $finalMessage . $proof;
  }

  /**
   * Hi() call, which is essentially PBKDF2 (RFC-2898) with HMAC-H() as the pseudorandom function.
   *
   * @param string $str  The string to hash.
   * @param string $salt The salt value.
   * @param int $i The   iteration count.
   */
  private function hi($str, $salt, $i)
  {
      $int1   = "\0\0\0\1";
      $ui     = $this->hmac($str, $salt . $int1, true);
      $result = $ui;
      for ($k = 1; $k < $i; $k++) {
          $ui     = $this->hmac($str, $ui, true);
          $result = $result ^ $ui;
      }
      return $result;
  }

  /**
   * SCRAM has also a server verification step. On a successful outcome, it will send additional data which must
   * absolutely be checked against this function. If this fails, the entity which we are communicating with is probably
   * not the server as it has not access to your ServerKey.
   *
   * @param string $data The additional data sent along a successful outcome.
   * @return bool Whether the server has been authenticated.
   * If false, the client must close the connection and consider to be under a MITM attack.
   */
  public function verify($data)
  {
      $verifierRegexp = '#^v=((?:[A-Za-z0-9/+]{4})*(?:[A-Za-z0-9/+]{3}=|[A-Za-z0-9/+]{2}==)?)$#';

      $matches = array();
      if (!isset($this->saltedPassword, $this->authMessage) || !preg_match($verifierRegexp, $data, $matches)) {
          // This cannot be an outcome, you never sent the challenge's response.
          return false;
      }

      $verifier                = $matches[1];
      $proposedServerSignature = base64_decode($verifier);
      $serverKey               = $this->hmac($this->saltedPassword, "Server Key", true);
      $serverSignature         = $this->hmac($serverKey, $this->authMessage, true);

      return $proposedServerSignature === $serverSignature;
  }

  /**
   * @return string
   */
  public function getCnonce()
  {
      return $this->cnonce;
  }

  public function getSaltedPassword()
  {
      return $this->saltedPassword;
  }

  public function getAuthMessage()
  {
      return $this->authMessage;
  }

  public function getHashAlgo()
  {
      return $this->hashAlgo;
  }

  private function hash($data) 
  {
    return hash('sha1', $data, true);
  }

  private function hmac($key, $str, $raw)
  {
    return hash_hmac('sha1', $str, $key, $raw);
  }

  private function generateCnonce()
  {
      foreach (array('/dev/urandom', '/dev/random') as $file) {
          if (is_readable($file)) {
              return base64_encode(file_get_contents($file, false, null, 0, 32));
          }
      }

      $cnonce = '';

      for ($i = 0; $i < 32; $i++) {
          $cnonce .= chr(mt_rand(0, 255));
      }

      return base64_encode($cnonce);
  }

  static function encodeCredentials($username, $password)
  {
    return \md5(\utf8_encode($username . ':mongo:' . $password));
  }
}