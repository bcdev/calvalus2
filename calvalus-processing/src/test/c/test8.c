#include <openssl/rsa.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/x509.h>
#include <cJSON.h>
#include <string.h>
#include <apr-1.0/apr_base64.h>

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

  /* read keys from files */

  RSA* calvalus_pub_rsa = read_pub("calvalus_pub.der");
  RSA* calvalus_priv_rsa = read_priv("calvalus_priv.der");
  //RSA* cas_pub_rsa = read_pub("cas_pub.der");
  //RSA* cas_priv_rsa = read_priv("cas_priv.der");

  unsigned char* clear = "messagefromc____";
  printf("clear=%s\n", clear);

  unsigned char encc[RSA_size(calvalus_pub_rsa)];
  int encclen = RSA_public_encrypt(16, clear, encc, calvalus_pub_rsa, RSA_PKCS1_OAEP_PADDING);
  int e = ERR_get_error();
  printf("1 %s\n", ERR_error_string(e, NULL));

  FILE* enccfile = fopen("encc.dat", "wb");
  fwrite(encc, encclen, 1, enccfile);
  fclose(enccfile);

  unsigned char decc[RSA_size(calvalus_priv_rsa)];
  int decclen = RSA_private_decrypt(encclen, encc, decc, calvalus_priv_rsa, RSA_PKCS1_OAEP_PADDING);
  e = ERR_get_error();
  printf("2 %s\n", ERR_error_string(e, NULL));
  decc[decclen] = '\0';
  printf("decc=%s\n", decc);

  unsigned char encj[256];
  FILE* encjfile = fopen("encj.dat", "rb");
  int encjlen = fread(encj, 1 /*(size_t) sizeof encj*/, 256, encjfile);
  fclose(encjfile);
  printf("encjlen=%d\n", encjlen);

  unsigned char decj[RSA_size(calvalus_priv_rsa)];
  int decjlen = RSA_private_decrypt(encjlen, encj, decj, calvalus_priv_rsa, RSA_PKCS1_OAEP_PADDING);
  e = ERR_get_error();
  printf("3 %s\n", ERR_error_string(e, NULL));
  decj[decjlen] = '\0';
  printf("decj=%s\n", decj);
}

