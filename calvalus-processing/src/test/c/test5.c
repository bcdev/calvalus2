#include <openssl/rsa.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/x509.h>
#include <cJSON.h>
#include <string.h>

void printhex(unsigned char* buffer, unsigned int length) {
  char hexlhash[length*2+1];
  int i;
  for (i=0; i<length; ++i) {
    sprintf(hexlhash+2*i, "%02x", buffer[i]);
  }
  printf("%s\n", hexlhash);
}

int do_hash(char* str, char* hash) {
  
  OpenSSL_add_all_digests();
  const EVP_MD* digest = EVP_get_digestbyname("sha256");
  int hashlen;
  EVP_MD_CTX* ctx = EVP_MD_CTX_create();
  EVP_DigestInit_ex(ctx, digest, NULL);
  EVP_DigestUpdate(ctx, str, strlen(str));
  EVP_DigestFinal_ex(ctx, hash, &hashlen);
  EVP_MD_CTX_destroy(ctx);
  return hashlen;
}

RSA* read_pub(const char* filename) {
  BIO* bio = BIO_new_file(filename, "rb");
  EVP_PKEY* pkey = d2i_PUBKEY_bio(bio, NULL);
  RSA* rsa = EVP_PKEY_get1_RSA(pkey);
  if (rsa == NULL) {
    int e = ERR_get_error();
    printf("%s\n", ERR_error_string(e, NULL));
  }
  OPENSSL_free(pkey);
  BIO_free(bio);
  return rsa;
}

RSA* read_priv(const char* filename) {
  BIO* bio = BIO_new_file(filename, "rb");
  EVP_PKEY* pkey = d2i_PrivateKey_bio(bio, NULL);
  RSA* rsa = EVP_PKEY_get1_RSA(pkey);
  if (rsa == NULL) {
    int e = ERR_get_error();
    printf("%s\n", ERR_error_string(e, NULL));
  }
  OPENSSL_free(pkey);
  BIO_free(bio);
  return rsa;
}


int main(int argc, char* argv[]) {

  ERR_load_crypto_strings();

  char* msg = "{\"user\":\"martin\",\"payload\":\"thisissomepayload\",\"saml\":{\"user\":\"martin\",\"email\":\"martin.boettcher@brockmann-consult.de\"}}";
  cJSON* root = cJSON_Parse(msg);

  /* access elements of message */

  char* user = cJSON_GetObjectItemCaseSensitive(root, "user")->valuestring;
  char* payload = cJSON_GetObjectItemCaseSensitive(root, "payload")->valuestring;
  char* saml = cJSON_PrintUnformatted(cJSON_GetObjectItemCaseSensitive(root, "saml"));

  printf("user=%s\n", user);
  printf("payload=%s\n", payload);
  printf("saml=%s\n", saml);

  /* hash payload data */
  /* echo -n thisissomepayload|sha256sum */

  unsigned char hash[EVP_MAX_MD_SIZE];
  int hashlen = do_hash(payload, hash);
  printhex(hash, hashlen);

  /* read keys from files */

  RSA* calvalus_pub_rsa = read_pub("calvalus_pub.der");
  RSA* calvalus_priv_rsa = read_priv("calvalus_priv.der");
  RSA* cas_pub_rsa = read_pub("cas_pub.der");
  RSA* cas_priv_rsa = read_priv("cas_priv.der");

  /* encrypt saml */

  char out1[RSA_size(calvalus_pub_rsa)];
  int i1 = RSA_public_encrypt(strlen(saml), saml, out1, calvalus_pub_rsa, RSA_PKCS1_OAEP_PADDING);
  /*
  char out2[RSA_size(cas_priv_rsa)];
  int i2 = RSA_private_encrypt(i1, out1, out2, cas_priv_rsa, RSA_PKCS1_PADDING);
    int e = ERR_get_error();
    printf("%s\n", ERR_error_string(e, NULL));
  */
  printf("rsa_encrypt returns %d\n", i1);
  printhex(out1, i1);

  /*
  char out3[RSA_size(cas_pub_rsa)];
  int i3 = RSA_public_decrypt(i2, out2, out3, cas_pub_rsa, RSA_PKCS1_PADDING);
  */
  char out4[RSA_size(calvalus_priv_rsa)];
  int i4 = RSA_private_decrypt(i1, out1, out4, calvalus_priv_rsa, RSA_PKCS1_OAEP_PADDING);
  out4[i4] = 0;

  printf("rsa_decrypt returns %d\n", i4);
  printf("%s\n", out4);
}

