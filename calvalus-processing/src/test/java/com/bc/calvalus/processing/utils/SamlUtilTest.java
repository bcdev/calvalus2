package com.bc.calvalus.processing.utils;

import org.apache.commons.codec.binary.Hex;
import org.joda.time.DateTime;
import org.junit.Ignore;
import org.junit.Test;
import org.opensaml.saml2.core.Response;
import org.opensaml.xml.ConfigurationException;
import org.opensaml.xml.security.credential.Credential;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SamlUtilTest
{
    public static final String EXPECTED2 = "dwC0UynjMKaq1tJaE1vrSDd1mRUzkgwNCzStlIDweJYr7iv2gzaWe8ZF9ps6cNl0bouY38DpuEPTesHoDsmCd917CzVnsiV" +
            "LrO9mN+F52eo2J8+ZXDapvHE4OTc5NocJYYYiAUE1Jcv1ZOfY1m/ZH5azYkDxdg8cEavBfxEz2t3tMtlL9BB3w1" +
            "tOpkIGbAT/KjwciBmD5Odo+i7S/pohcu47J5/l6+ztxV5WOUVXGo2pxwlDwo/FV01kGQplLH5KrbWSasXpOdwgy" +
            "sgKog+a+VjaMMkLgtO6rKSpAaE9jnrNQz1gUF8FXTeV5zMD19hSF/ViqYmdHBiqIIh3rzIpOA== 4VqAhDbk4qg" +
            "vnURtC5lu9D1QcUc0LoUcetRN5eeOkMVSJ52sjgrPsD6DHdqurvzQvlYuASRKnxnjaAd9L4+vzDA6zWn/TPKQM/" +
            "wSsKA726mJMhpoWNgxJmMlR5YlwdNm/GV7ZeRlVzo8nNhQ/aHoX9CnTOdjID1wgMJN3wCwHgJExBXyNStByjdB/" +
            "tFReDVtC62FrhI8mRQoiDQDttzLdx0nufOEs/puxuS9xY6NXjOjBeNfvkDxiknYzhtcuOef2D2TdonfsVABHUIz" +
            "lxPGGVmUm+DHKvgbqFnCgnIo96LufqJx3QU71JDlbMteTQaKE/ygKIU/xz28aqr6sqXzGgBU3JdjzMPIvgLaBJO" +
            "Ukt0O2LVL8jTRyHiKS97CMOlYxJb+9d/uf4tsB6v9PO2t9eDDwHXD+5ygxSgdIn1wOA/OsAX3+WDyyfEbSu9ACD" +
            "eBWCUndmlXK11vbcUIs4p1RIpcWHYM9xkhdOv5qQMPBYZYtokBcWupm1Y/JzKdoMXLgEQtXqIw9FNBYztUL+0Tr" +
            "GsBG9l3sb2VePeEOXdFVEh+PPt1yW6+NlyBVtq2QRHyI1z2ncCTSi6Kjnp7/CqYZRIHM/iPkcPSMj+udiepsSMz" +
            "2RuJYciJxHdhdrM6ImaeaxHY4pSJG1v8+Z09nSgsQA+RWz+7It7uxvaM73JQaByiDbq3K0QPlPjiNjDiDT37wrO" +
            "zuohOuUxA3AnlhFIVSbw+xlhfTJsBrtYGb3f1yAPsn1Spm5jhjDACEtpWFs8/vg980IDdzZIYurAG8pGElTbZJQ" +
            "GmWaKuT+0D+7ufunN7Xg2tpm2P2ckid+menujPzreHE+G/I9EslTDYpJTSfTC0ef7UPblc2xAITUSFGVLPosW4f" +
            "JCYPIj2ZEQPsQ213rTZe2DB4QVTy/GJYjuZVPGEwMNjkoY/T+ISoZKZsvsIWfMatbsTFs7dhG+iCXkMQfpPFzmp" +
            "jH/X52v0U100g9ZIsYdP5mAbgFlS0iXuvAt0K3Wu7fk8hLWT8FE0RAvQabEWuVRuhjrTR7OBjl6JF7WpWQ0R5hJ" +
            "/Nj96giBlI3/MbkLZ6gHRTUw8fYAJUdRiqRQUJeSqLJk7cOzd01Tlalaj0woqahDMNe+ZAi3sDxOaRFrAB4hmGt" +
            "JyGlXIKreBh46tT3TveuwNbVl6pqglyTGVKHeSFxoHkeOcgTT0shjhfEfKj859/LYF+N6XZnR4QfpPFzmpjH/X5" +
            "2v0U100gyhlL+wiwXP7Mllxc+mAqriuLpCY5YclV5BQKl/jH7QVfgIHGqEeU90KiGA7RUrH/ocikHroq4DFKZGL" +
            "wk3A1gBuqrLXFWr8sFwGGoDE9l9TGyUXn4pEc3SPbPLHuJRQSn2If7EdXSlLJXh8arXPRtkN/UDAf/1TqTkVycw" +
            "ZCoBLsELn+O+QqvFu76y+gw394UkmPlOmfmLcIGFdAzJ7aj62w0lqw7XeiBHr0kpjQ6iIhntHdKQ5CI2y0WcJMk" +
            "ImQHjzRIigUkEi4JmV1XjvYn47QfQjBPgUokn5ejjxOHvtA2c5BLkmxR1lNOpO2IptWOV3Qgavz16YdN0UTXiEY" +
            "fC2Lg7K+/z5VQcYhD5JAC06JblvhCn7DBP4ek31Jg3rtFOVqtSxjc3vjOWSvPdWJ+/AyYzYIvqUngwH1pNJNNcv" +
            "9iyTrk8AxswenArDqkC1DmZfc4FP834n02IN4eSsBQZihtBYFWmngKqwzMBc+mcP8Mq5F7Wn+clFle8n/LeqYqz" +
            "oo1Q/1duI8x+vXSJ5UCNTdjUamUl3lHBQa1HvPD7WCtXCZTj53QQyS62zEO/cJqbsg8zsiXuzl9WMJyTaF9IFoC" +
            "MBni8Q7maxpJgHPpsED3zwtl4U8K0FnPivsgTlhv0eH6my71rCJ2vs1cNkT0OaE1EKhaRpFQFdQzl3/bb/zdXSh" +
            "vJCtHYs7MD96UayPgw1dSBzWYTZzpw7FhyPI1jlsbhXiYRMKQHeRO9tpaIMhBtTUWzh0ks3nEb/j24Yd+DYMXt+" +
            "h3LfLUz5PpF7qsCpPhzwXkr4Hnlm9KtbVrlc3xmtDU2Ael4uFxbHMeuM1AJ98+ZuNWTgTwMY+VF8s1wg+z8rFCI" +
            "2AaEI50zNqhpbAuFVo1CfGnODImFsS9aTAoGALZ/CyFW/Nacqp2hjssVoXMoI4khjfUuFzyLioKRa570cmmkpM2" +
            "6hV44Ipg+pkfioLCgqtnMJJKB2L82+eDYPe+nYd7MPq0CLR4ajqGEq6RYj+n7LahlOQ7F0v7LwOJt+pVsSc1jfj" +
            "SganbwyRfZPfVIe83xlkkCuHiyQaP4K5S0SDky0qzP+4M3B2uli/3X7n9x9bYUpXVVFyNbwBPDQHVDv7S2xPi8V" +
            "2YCQ8oqVf/AmDjBnCn5467QcGPvxJ824chPL05N9GZ+yjgH+3dsTVjt7ZzrKt5dXeMJKV5+yyVTVhv3YT8gjo2P" +
            "wUgVYOjjqBzn1lcfRNZcl7NVXNcuuZeJBN2uKVUtAHw3qP9drJCzIJEdGskmx5rYmIBpsO/n9rCSpOZ/EDG2tP9" +
            "j8KpsEJ+dUYwJGBzW7VKJaueeP/af/MoU9vMYwSzvek9X2VgnrCln2t0AZeXijSxJ2qei5JeJhZ+y6qiKy8Y4R6" +
            "ROJ6HuJeau/bIvNplPCDkezFOhxyGKUJiqCdNUNs2hhHvyAoRmxANrYHGpDWjB0Fk8GWDHpQ0HEmZbRmiayuYIt" +
            "7BYoJrsp4EqM+sOv8I+S09Hqx/xqrNkdQULWvcAliJu4ONz22TdZptLR3T5W+bmU0GsiIpeW9+MCDn9dbbx7dL/" +
            "K3I6QHQq+CdEFwSd+6ulbDim6kw1BxewBvcOkCVmG+y3PBGgcAwm4KII8xbiBIktnWS8L20HvGSNOY5fn49XnDn" +
            "e4vETyij7CBeBrtgvFEFNmM8AvbI775UaBRtQ86rQi7lFYKZKDiEgVidOB047xxTZ5Eb05GO3eCpkgCLoPDs6FR" +
            "4oVfUn5vJ7mh5r8QSK1l0Ot2gvX1weWNvAce0mOWHbguqHOHB++b2Cp2dajxnNxGYyIQ7qlkzPP5Wsr+PLTQt7g" +
            "/t8apqRVWZ/Z7zEgdq7cCK7bjVmxcH+Nei1+i03ooJ1LH9eKkNddoH0hfVkWnaJCvzpYVLcD8HvrNSwd6jYlcrS" +
            "7GWcrmSE8TVzctCoXgOdO229PE2UKhYvEBlaOxfYjiGERpVteAD5/CfnmOW5N9f1QhTAW8cmjaXh32Q+/1BBSzh" +
            "Z4PmK2NF7hHjabcqe2kdgIktjGrEkGsVHnL/HIiXbCLsOp8xQ/lGVg3Cu015q79Ut+HdjfLQDz7Qwdbo8LPUt9e" +
            "GYojdFWHa96yt29GzbRSnRA9LWI7WZUFP9UgI6SL1uuntR0LU4IMfvokpONAY7+ua6DoavpPHfAs+XIG6egnPd7" +
            "rHiQVOJTMIKKuOGo/vEwtY1dzigT01za8cJjZwRiug1VvS4BgIPM4fPt3zc7anqa4tuzea2ymS0uVUOEIabqwQb" +
            "7bjQeRC2aMczTUYOBo7NdtRjUoK3egl05w7q/S8eD+TuHVdQxQNGjaZrXIQV7IuHblUxZ1wvs09gAOtYSqxK2MU" +
            "SFWuvTYX4v3Q6GwQf4ko3Yhz8ZJPJEm/eN6EnPgmQjxwd1JEAyYsc1SuCXem2nLov9z+83asZYsXLvaKyFR2LzR" +
            "+KQc8FXGxDTpjXyqoKItKU2EoniIbdIfoFicjvzWvGaDzg8DyROYn8Gvt1TqYOaazuRZgFEfDBP04tzUnb49DDB" +
            "ygHElv713+5/i2wHq/087a31tUZNyd81RlirRdqS5HqH13dwMLOWUosMLMSGmDUXC77iq+fJdYfeB7UWbNIOfDA" +
            "oxnQqdxmCCP/TVNtnKAuc+d0fYlqHqUmjRYxzi5r0snHNXXw6eYYCgPIIFonAEh4xJYQN+YEz7pls9mK2iblEBc" +
            "mff9obkZrEh4kv06u9j3Dad4SPRkYh92+YaJoVZ50LNlHeFhp3bo63thDuDlQGvfrDgKU7k4EKCQgQ77yrhjqmh" +
            "z1wTVJy5p2wLvCKT+yeEBcEw4FcpN8AQU3DFpOFdhr0NWp5eFBCy5+ThCyReFnb1P2dhEW2R9/UfaR/rfqzdWgV" +
            "mpOK/lQO0msa86GHZyGjxQWYPYtU9OKDbWnt4f5eZWctSxJVH6P6ywvDBTWzvxY2++tpHTpIvxChtFNaGOvgdqQ" +
            "tiFK0KX2szgXUed8uRcKfQFxJ/EX4zIK0NJplz7GJY1V7isrrJR1eSKg2BqzBi5otNkkqcyDYs97o1uM=";
    public static final String EXPECTED1 = "dwC0UynjMKaq1tJaE1vrSDd1mRUzkgwNCzStlIDweJYr7iv2gzaWe8ZF9ps6cNl0bouY38DpuEPTesHoDsmCd917CzVnsiV" +
            "LrO9mN+F52eo2J8+ZXDapvHE4OTc5NocJYYYiAUE1Jcv1ZOfY1m/ZH5azYkDxdg8cEavBfxEz2t3tMtlL9BB3w1" +
            "tOpkIGbAT/KjwciBmD5Odo+i7S/pohcu47J5/l6+ztxV5WOUVXGo2pxwlDwo/FV01kGQplLH5KrbWSasXpOdwgy" +
            "sgKog+a+VjaMMkLgtO6rKSpAaE9jnrNQz1gUF8FXTeV5zMD19hSF/ViqYmdHBiqIIh3rzIpOA== 4VqAhDbk4qg" +
            "vnURtC5lu9D1QcUc0LoUcetRN5eeOkMVSJ52sjgrPsD6DHdqurvzQvlYuASRKnxnjaAd9L4+vzDA6zWn/TPKQM/" +
            "wSsKA726mJMhpoWNgxJmMlR5YlwdNm/GV7ZeRlVzo8nNhQ/aHoX9CnTOdjID1wgMJN3wCwHgJExBXyNStByjdB/" +
            "tFReDVtC62FrhI8mRQoiDQDttzLdx0nufOEs/puxuS9xY6NXjOjBeNfvkDxiknYzhtcuOef2D2TdonfsVABHUIz" +
            "lxPGGVmUm+DHKvgbqFnCgnIo96LufqJx3QU71JDlbMteTQaKE/ygKIU/xz28aqr6sqXzGgBU3JdjzMPIvgLaBJO" +
            "Ukt0O2LVL8jTRyHiKS97CMOlYxJb+9d/uf4tsB6v9PO2t9eDDwHXD+5ygxSgdIn1wOA/OsAX3+WDyyfEbSu9ACD" +
            "eBWCUndmlXK11vbcUIs4p1RIpcWHYM9xkhdOv5qQMPBYZYtokBcWupm1Y/JzKdoMXLgEQtXqIw9FNBYztUL+0Tr" +
            "GsBG9l3sb2VePeEOXdFVEh+PPt1yW6+NlyBVtq2QRHyI1z2ncCTSi6Kjnp7/CqYZRIHM/iPkcPSMj+udiepsSMz" +
            "2RuJYciJxHdhdrM6ImaeaxHY4pSJG1v8+Z09nSgsQA+RWz+7It7uxvaM73JQaByiDbq3K0QPlPjiNjDiDT37wrO" +
            "zuohOuUxA3AnlhFIVSbw+xlhfTJsBrtYGb3f1yAPsn1Spm5jhjDACEtpWFs8/vg980IDdzZIYurAG8pGElTbZJQ" +
            "GmWaKuT+0D+7ufunN7Xg2tpm2P2ckid+menujPzreHE+G/I9EslTDYpJTSfTC0ef7UPblc2xAITUSFGVLPosW4f" +
            "JCYPIj2ZEQPsQ213rTZe2DB4QVTy/GJYjuZVPGEwMNjkoY/T+ISoZKZsvsIWfMatbsTFs7dhG+iCXkMQfpPFzmp" +
            "jH/X52v0U100g9ZIsYdP5mAbgFlS0iXuvAt0K3Wu7fk8hLWT8FE0RAvQabEWuVRuhjrTR7OBjl6JF7WpWQ0R5hJ" +
            "/Nj96giBlI3/MbkLZ6gHRTUw8fYAJUdRiqRQUJeSqLJk7cOzd01Tlalaj0woqahDMNe+ZAi3sDxOaRFrAB4hmGt" +
            "JyGlXIKreBh46tT3TveuwNbVl6pqglyTGVKHeSFxoHkeOcgTT0shjhfEfKj859/LYF+N6XZnR4QfpPFzmpjH/X5" +
            "2v0U100gyhlL+wiwXP7Mllxc+mAqriuLpCY5YclV5BQKl/jH7QVfgIHGqEeU90KiGA7RUrH/ocikHroq4DFKZGL" +
            "wk3A1gBuqrLXFWr8sFwGGoDE9l9TGyUXn4pEc3SPbPLHuJRQSn2If7EdXSlLJXh8arXPRtkN/UDAf/1TqTkVycw" +
            "ZCoBLsELn+O+QqvFu76y+gw394UkmPlOmfmLcIGFdAzJ7aj62w0lqw7XeiBHr0kpjQ6iIhntHdKQ5CI2y0WcJMk" +
            "ImQHjzRIigUkEi4JmV1XjvYn47QfQjBPgUokn5ejjxOHvtA2c5BLkmxR1lNOpO2IptWOV3Qgavz16YdN0UTXiEY" +
            "fC2Lg7K+/z5VQcYhD5JAC06JblvhCn7DBP4ek31Jg3rtFOVqtSxjc3vjOWSvPdWJ+/AyYzYIvqUngwH1pNJNNcv" +
            "9iyTrk8AxswenArDqkC1DmZfc4FP834n02IN4eSsBQZihtBYFWmngKqwzMBc+mcP8Mq5F7Wn+clFle8n/LeqYqz" +
            "oo1Q/1duI8x+vXSJ5UCNTdjUamUl3lHBQa1HvPD7WCtXCZTj53QQyS62zEO/cJqbsg8zsiXuzl9WMJyTaF9IFoC" +
            "MBni8Q7maxpJgHPpsED3zwtl4U8K0FnPivsgTlhv0eH6my71rCJ2vs1cNkT0OaE1EKhaRpFQFdQzl3/bb/zdXSh" +
            "vJCtHYs7MD96UayPgw1dSBzWYTZzpw7FhyPI1jlsbhXiYRMKQHeRO9tpaIMhBtTUWzh0ks3nEb/j24Yd+DYMXt+" +
            "h3LfLUz5PpF7qsCpPhzwXkr4Hnlm9KtbVrlc3xmtDU2Ael4uFxbHMeuM1AJ98+ZuNWTgTwMY+VF8s1wg+z8rFCI" +
            "2AaEI50zNqhpbAuFVo1CfGnODImFsS9aTAoGALZ/CyFW/Nacqp2hjssVoXMoI4khjfUuFzyLioKRa570cmmkpM2" +
            "6hV44Ipg+pkfioLCgqtnMJJKB2L82+eDYPe+nYd7MPq0CLR4ajqGEq6RYj+n7LahlOQ7F0v7LwOJt+pVsSc1jfj" +
            "SganbwyRfZPfVIe83xlkkCuHiyQaP4K5S0SDky0qzP+4M3B2uli/3X7n9x9bYUpXVVFyNbwBPDQHVDv7S2xPi8V" +
            "2YCQ8oqVf/AmDjBnCn5467QcGPvxJ824chPL05N9GZ+yjgH+3dsTVjt7ZzrKt5dXeMJKV5+yyVTVhv3YT8gjo2P" +
            "wUgVYOjjqBzn1lcfRNZcl7NVXNcuuZeJBN2uKVUtAHw3qP9drJCzIJEdGskmx5rYmIBpsO/n9rCSpOZ/EDG2tP9" +
            "j8KpsEJ+dUYwJGBzW7VKJaueeP/af/MoU9vMYwSzvek9X2VgnrCln2t0AZeXijSxJ2qei5JeJhZ+y6qiKy8Y4R6" +
            "ROJ6HuJeau/bIvNplPCDkezFOhxyGKUJiqCdNUNs2hhHvyAoRmxANrYHGpDWjB0Fk8GWDHpQ0HEmZbRmiayuYIt" +
            "7BYoJrsp4EqM+sOv8I+S09Hqx/xqrNkdQULWvcAliJu4ONz22TdZptLR3T5W+bmU0GsiIpeW9+MCDn9dbbx7dL/" +
            "K3I6QHQq+CdEFwSd+6ulbDim6kw1BxewBvcOkCVmG+y3PBGgcAwm4KII8xbiBIktnWS8L20HvGSNOY5fn49XnDn" +
            "e4vETyij7CBeBrtgvFEFNmM8AvbI775UaBRtQ86rQi7lFYKZKDiEgVidOB047xxTZ5Eb05GO3eCpkgCLoPDs6FR" +
            "4oVfUn5vJ7mh5r8QSK1l0Ot2gvX1weWNvAce0mOWHbguqHOHB++b2Cp2dajxnNxGYyIQ7qlkzPP5Wsr+PLTQt7g" +
            "/t8apqRVWZ/Z7zEgdq7cCK7bjVmxcH+Nei1+i03ooJ1LH9eKkNddoH0hfVkWnaJCvzpYVLcD8HvrNSwd6jYlcrS" +
            "7GWcrmSE8TVzctCoXgOdO229PE2UKhYvEBlaOxfYjiGERpVteAD5/CfnmOW5N9f1QhTAW8cmjaXh32Q+/1BBSzh" +
            "Z4PmK2NF7hHjabcqe2kdgIktjGrEkGsVHnL/HIiXbCLsOp8xQ/lGVg3Cu015q79Ut+HdjfLQDz7Qwdbo8LPUt9e" +
            "GYojdFWHa96yt29GzbRSnRA9LWI7WZUFP9UgI6SL1uuntR0LU4IMfvokpONAY7+ua6DoavpPHfAs+XIG6egnPd7" +
            "rHiQVOJTMIKKuOGo/vEwtY1dzigT01za8cJjZwRiug1VvS4BgIPM4fPt3zc7anqa4tuzea2ymS0uVUOEIabqwQb" +
            "7bjQeRC2aMczTUYOBo7NdtRjUoK3egl05w7q/S8eD+TuHVdQxQNGjaZrXIQV7IuHblUxZ1wvs09gAOtYSqxK2MU" +
            "SFWuvTYX4v3Q6GwQf4ko3Yhz8ZJPJEm/eN6EnPgmQjxwd1JEAyYsc1SuCXem2nLov9z+83asZYsXLvaKyFR2LzR" +
            "+KQc8FXGxDTpjXyqoKItKU2EoniIbdIfoFicjvzWvGaDzg8DyROYn8Gvt1TqYOaazuRZgFEfDBP04tzUnb49DDB" +
            "ygHElv713+5/i2wHq/087a31tUZNyd81RlirRdqS5HqH13dwMLOWUosMLMSGmDUXC77iq+fJdYfeB7UWbNIOfDA" +
            "oxnQqdxmCCP/TVNtnKAuc+d0fYlqHqUmjRYxzi5r0snHNXXw6eYYCgPIIFonAEh4xJYQN+YEz7pls9mK2iblEBc" +
            "mff9obkZrEh4kv06u9j3Dad4SPRkYh92+YaJoVZ50LNlHeFhp3bo63thDuDlQGvfrDgKU7k4EKCQgQ77yrhjqmh" +
            "z1wTVJy5p2wLvCKT+yeEBcEw4FcpN8AQU3DFpOFdhr0NWp5eFBCy5+ThCyReFnb1P2dhEW2R9/UfaR/rfqzdWgV" +
            "mpOK/lQO0msa86GHZyGjxQWYPYtU9OKDbWnt4f5eZWctSxJVH6P6ywvDBTWzvxY2++tpHTpIvxChtFNaGOvgdqQ" +
            "tiFK0KX2szgXUed8uRcKfQFxJ/EX4zIK0NJplz7GJY1V7isrrJR1eSKg2BqzBi5otNkkqcyDYs97o1uM=";
    public static final String EXPECTED = "dwC0UynjMKaq1tJaE1vrSDd1mRUzkgwNCzStlIDweJYr7iv2gzaWe8ZF9ps6cNl0bouY38DpuEPTesHoDsmCd917CzVnsiV" +
            "LrO9mN+F52eo2J8+ZXDapvHE4OTc5NocJYYYiAUE1Jcv1ZOfY1m/ZH5azYkDxdg8cEavBfxEz2t3tMtlL9BB3w1" +
            "tOpkIGbAT/KjwciBmD5Odo+i7S/pohcu47J5/l6+ztxV5WOUVXGo2pxwlDwo/FV01kGQplLH5KrbWSasXpOdwgy" +
            "sgKog+a+VjaMMkLgtO6rKSpAaE9jnrNQz1gUF8FXTeV5zMD19hSF/ViqYmdHBiqIIh3rzIpOA== 4VqAhDbk4qg" +
            "vnURtC5lu9D1QcUc0LoUcetRN5eeOkMVSJ52sjgrPsD6DHdqurvzQvlYuASRKnxnjaAd9L4+vzDA6zWn/TPKQM/" +
            "wSsKA726mJMhpoWNgxJmMlR5YlwdNm/GV7ZeRlVzo8nNhQ/aHoX9CnTOdjID1wgMJN3wCwHgJExBXyNStByjdB/" +
            "tFReDVtC62FrhI8mRQoiDQDttzLdx0nufOEs/puxuS9xY6NXjOjBeNfvkDxiknYzhtcuOef2D2TdonfsVABHUIz" +
            "lxPGGVmUm+DHKvgbqFnCgnIo96LufqJx3QU71JDlbMteTQaKE/ygKIU/xz28aqr6sqXzGgBU3JdjzMPIvgLaBJO" +
            "Ukt0O2LVL8jTRyHiKS97CMOlYxJb+9d/uf4tsB6v9PO2t9eDDwHXD+5ygxSgdIn1wOA/OsAX3+WDyyfEbSu9ACD" +
            "eBWCUndmlXK11vbcUIs4p1RIpcWHYM9xkhdOv5qQMPBYZYtokBcWupm1Y/JzKdoMXLgEQtXqIw9FNBYztUL+0Tr" +
            "GsBG9l3sb2VePeEOXdFVEh+PPt1yW6+NlyBVtq2QRHyI1z2ncCTSi6Kjnp7/CqYZRIHM/iPkcPSMj+udiepsSMz" +
            "2RuJYciJxHdhdrM6ImaeaxHY4pSJG1v8+Z09nSgsQA+RWz+7It7uxvaM73JQaByiDbq3K0QPlPjiNjDiDT37wrO" +
            "zuohOuUxA3AnlhFIVSbw+xlhfTJsBrtYGb3f1yAPsn1Spm5jhjDACEtpWFs8/vg980IDdzZIYurAG8pGElTbZJQ" +
            "GmWaKuT+0D+7ufunN7Xg2tpm2P2ckid+menujPzreHE+G/I9EslTDYpJTSfTC0ef7UPblc2xAITUSFGVLPosW4f" +
            "JCYPIj2ZEQPsQ213rTZe2DB4QVTy/GJYjuZVPGEwMNjkoY/T+ISoZKZsvsIWfMatbsTFs7dhG+iCXkMQfpPFzmp" +
            "jH/X52v0U100g9ZIsYdP5mAbgFlS0iXuvAt0K3Wu7fk8hLWT8FE0RAvQabEWuVRuhjrTR7OBjl6JF7WpWQ0R5hJ" +
            "/Nj96giBlI3/MbkLZ6gHRTUw8fYAJUdRiqRQUJeSqLJk7cOzd01Tlalaj0woqahDMNe+ZAi3sDxOaRFrAB4hmGt" +
            "JyGlXIKreBh46tT3TveuwNbVl6pqglyTGVKHeSFxoHkeOcgTT0shjhfEfKj859/LYF+N6XZnR4QfpPFzmpjH/X5" +
            "2v0U100gyhlL+wiwXP7Mllxc+mAqriuLpCY5YclV5BQKl/jH7QVfgIHGqEeU90KiGA7RUrH/ocikHroq4DFKZGL" +
            "wk3A1gBuqrLXFWr8sFwGGoDE9l9TGyUXn4pEc3SPbPLHuJRQSn2If7EdXSlLJXh8arXPRtkN/UDAf/1TqTkVycw" +
            "ZCoBLsELn+O+QqvFu76y+gw394UkmPlOmfmLcIGFdAzJ7aj62w0lqw7XeiBHr0kpjQ6iIhntHdKQ5CI2y0WcJMk" +
            "ImQHjzRIigUkEi4JmV1XjvYn47QfQjBPgUokn5ejjxOHvtA2c5BLkmxR1lNOpO2IptWOV3Qgavz16YdN0UTXiEY" +
            "fC2Lg7K+/z5VQcYhD5JAC06JblvhCn7DBP4ek31Jg3rtFOVqtSxjc3vjOWSvPdWJ+/AyYzYIvqUngwH1pNJNNcv" +
            "9iyTrk8AxswenArDqkC1DmZfc4FP834n02IN4eSsBQZihtBYFWmngKqwzMBc+mcP8Mq5F7Wn+clFle8n/LeqYqz" +
            "oo1Q/1duI8x+vXSJ5UCNTdjUamUl3lHBQa1HvPD7WCtXCZTj53QQyS62zEO/cJqbsg8zsiXuzl9WMJyTaF9IFoC" +
            "MBni8Q7maxpJgHPpsED3zwtl4U8K0FnPivsgTlhv0eH6my71rCJ2vs1cNkT0OaE1EKhaRpFQFdQzl3/bb/zdXSh" +
            "vJCtHYs7MD96UayPgw1dSBzWYTZzpw7FhyPI1jlsbhXiYRMKQHeRO9tpaIMhBtTUWzh0ks3nEb/j24Yd+DYMXt+" +
            "h3LfLUz5PpF7qsCpPhzwXkr4Hnlm9KtbVrlc3xmtDU2Ael4uFxbHMeuM1AJ98+ZuNWTgTwMY+VF8s1wg+z8rFCI" +
            "2AaEI50zNqhpbAuFVo1CfGnODImFsS9aTAoGALZ/CyFW/Nacqp2hjssVoXMoI4khjfUuFzyLioKRa570cmmkpM2" +
            "6hV44Ipg+pkfioLCgqtnMJJKB2L82+eDYPe+nYd7MPq0CLR4ajqGEq6RYj+n7LahlOQ7F0v7LwOJt+pVsSc1jfj" +
            "SganbwyRfZPfVIe83xlkkCuHiyQaP4K5S0SDky0qzP+4M3B2uli/3X7n9x9bYUpXVVFyNbwBPDQHVDv7S2xPi8V" +
            "2YCQ8oqVf/AmDjBnCn5467QcGPvxJ824chPL05N9GZ+yjgH+3dsTVjt7ZzrKt5dXeMJKV5+yyVTVhv3YT8gjo2P" +
            "wUgVYOjjqBzn1lcfRNZcl7NVXNcuuZeJBN2uKVUtAHw3qP9drJCzIJEdGskmx5rYmIBpsO/n9rCSpOZ/EDG2tP9" +
            "j8KpsEJ+dUYwJGBzW7VKJaueeP/af/MoU9vMYwSzvek9X2VgnrCln2t0AZeXijSxJ2qei5JeJhZ+y6qiKy8Y4R6" +
            "ROJ6HuJeau/bIvNplPCDkezFOhxyGKUJiqCdNUNs2hhHvyAoRmxANrYHGpDWjB0Fk8GWDHpQ0HEmZbRmiayuYIt" +
            "7BYoJrsp4EqM+sOv8I+S09Hqx/xqrNkdQULWvcAliJu4ONz22TdZptLR3T5W+bmU0GsiIpeW9+MCDn9dbbx7dL/" +
            "K3I6QHQq+CdEFwSd+6ulbDim6kw1BxewBvcOkCVmG+y3PBGgcAwm4KII8xbiBIktnWS8L20HvGSNOY5fn49XnDn" +
            "e4vETyij7CBeBrtgvFEFNmM8AvbI775UaBRtQ86rQi7lFYKZKDiEgVidOB047xxTZ5Eb05GO3eCpkgCLoPDs6FR" +
            "4oVfUn5vJ7mh5r8QSK1l0Ot2gvX1weWNvAce0mOWHbguqHOHB++b2Cp2dajxnNxGYyIQ7qlkzPP5Wsr+PLTQt7g" +
            "/t8apqRVWZ/Z7zEgdq7cCK7bjVmxcH+Nei1+i03ooJ1LH9eKkNddoH0hfVkWnaJCvzpYVLcD8HvrNSwd6jYlcrS" +
            "7GWcrmSE8TVzctCoXgOdO229PE2UKhYvEBlaOxfYjiGERpVteAD5/CfnmOW5N9f1QhTAW8cmjaXh32Q+/1BBSzh" +
            "Z4PmK2NF7hHjabcqe2kdgIktjGrEkGsVHnL/HIiXbCLsOp8xQ/lGVg3Cu015q79Ut+HdjfLQDz7Qwdbo8LPUt9e" +
            "GYojdFWHa96yt29GzbRSnRA9LWI7WZUFP9UgI6SL1uuntR0LU4IMfvokpONAY7+ua6DoavpPHfAs+XIG6egnPd7" +
            "rHiQVOJTMIKKuOGo/vEwtY1dzigT01za8cJjZwRiug1VvS4BgIPM4fPt3zc7anqa4tuzea2ymS0uVUOEIabqwQb" +
            "7bjQeRC2aMczTUYOBo7NdtRjUoK3egl05w7q/S8eD+TuHVdQxQNGjaZrXIQV7IuHblUxZ1wvs09gAOtYSqxK2MU" +
            "SFWuvTYX4v3Q6GwQf4ko3Yhz8ZJPJEm/eN6EnPgmQjxwd1JEAyYsc1SuCXem2nLov9z+83asZYsXLvaKyFR2LzR" +
            "+KQc8FXGxDTpjXyqoKItKU2EoniIbdIfoFicjvzWvGaDzg8DyROYn8Gvt1TqYOaazuRZgFEfDBP04tzUnb49DDB" +
            "ygHElv713+5/i2wHq/087a31tUZNyd81RlirRdqS5HqH13dwMLOWUosMLMSGmDUXC77iq+fJdYfeB7UWbNIOfDA" +
            "oxnQqdxmCCP/TVNtnKAuc+d0fYlqHqUmjRYxzi5r0snHNXXw6eYYCgPIIFonAEh4xJYQN+YEz7pls9mK2iblEBc" +
            "mff9obkZrEh4kv06u9j3Dad4SPRkYh92+YaJoVZ50LNlHeFhp3bo63thDuDlQGvfrDgKU7k4EKCQgQ77yrhjqmh" +
            "z1wTVJy5p2wLvCKT+yeEBcEw4FcpN8AQU3DFpOFdhr0NWp5eFBCy5+ThCyReFnb1P2dhEW2R9/UfaR/rfqzdWgV" +
            "mpOK/lQO0msa86GHZyGjxQWYPYtU9OKDbWnt4f5eZWctSxJVH6P6ywvDBTWzvxY2++tpHTpIvxChtFNaGOvgdqQ" +
            "tiFK0KX2szgXUed8uRcKfQFxJ/EX4zIK0NJplz7GJY1V7isrrJR1eSKg2BqzBi5otNkkqcyDYs97o1uM=";
    SamlUtil util;
    public SamlUtilTest() throws NoSuchAlgorithmException, ConfigurationException {
        util = new SamlUtil();
    }

    static String SIMPLE_SAML_RESPONSE =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<saml2p:Response Version=\"2.0\" xmlns:saml2p=\"urn:oasis:names:tc:SAML:2.0:protocol\">\n" +
        "    <saml2:Assertion Version=\"2.0\" xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\">\n" +
        "        <saml2:Issuer>cas</saml2:Issuer>\n" +
        "        <saml2:Subject>\n" +
        "            <saml2:NameID>cd_calvalus</saml2:NameID>\n" +
        "        </saml2:Subject>\n" +
        "        <saml2:Conditions NotBefore=\"2017-06-23T10:27:05.354Z\" NotOnOrAfter=\"2017-06-24T10:27:05.354Z\"/>\n" +
        "        <saml2:AuthnStatement>\n" +
        "            <saml2:AuthnContext>\n" +
        "                <saml2:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:Password</saml2:AuthnContextClassRef>\n" +
        "            </saml2:AuthnContext>\n" +
        "        </saml2:AuthnStatement>\n" +
        "        <saml2:AttributeStatement>\n" +
        "            <saml2:Attribute Name=\"groups\">\n" +
        "                <saml2:AttributeValue\n" +
        "                    xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"\n" +
        "                    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalus,testproject</saml2:AttributeValue>\n" +
        "            </saml2:Attribute>\n" +
        "            <saml2:Attribute Name=\"email\">\n" +
        "                <saml2:AttributeValue\n" +
        "                    xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"\n" +
        "                    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalustest@code.de</saml2:AttributeValue>\n" +
        "            </saml2:Attribute>\n" +
        "        </saml2:AttributeStatement>\n" +
        "    </saml2:Assertion>\n" +
        "</saml2p:Response>\n";

    static String SIGNED_SAML_RESPONSE =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<saml2p:Response Version=\"2.0\" xmlns:saml2p=\"urn:oasis:names:tc:SAML:2.0:protocol\">\n" +
        "    <saml2:Assertion Version=\"2.0\"\n" +
        "        xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">\n" +
        "        <saml2:Issuer>cas</saml2:Issuer>\n" +
        "        <ds:Signature xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\">\n" +
        "            <ds:SignedInfo>\n" +
        "                <ds:CanonicalizationMethod Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/>\n" +
        "                <ds:SignatureMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#dsa-sha1\"/>\n" +
        "                <ds:Reference URI=\"\">\n" +
        "                    <ds:Transforms>\n" +
        "                        <ds:Transform Algorithm=\"http://www.w3.org/2000/09/xmldsig#enveloped-signature\"/>\n" +
        "                        <ds:Transform Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\">\n" +
        "                            <ec:InclusiveNamespaces PrefixList=\"xs\" xmlns:ec=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/>\n" +
        "                        </ds:Transform>\n" +
        "                    </ds:Transforms>\n" +
        "                    <ds:DigestMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#sha1\"/>\n" +
        "                    <ds:DigestValue>jsFpcQ9z8tMHdtk0/oYwP8kAxBc=</ds:DigestValue>\n" +
        "                </ds:Reference>\n" +
        "            </ds:SignedInfo>\n" +
        "            IEYJZHtDALBgcqhkjOOAQDBQAwbDEQMA4GA1UEBhMHVW5rbm93bjEQMA4G\n" +
        "A1UECBMHVW5rbm93</ds:SignatureValue>\n" +
        "            <ds:KeyInfo>\n" +
        "                <ds:X509Data>\n" +
        "                    <ds:X509Certificate>MIIDNTCCAvOgAwIBAgIEYJZHtDALBgcqhkjOOAQDBQAwbDEQMA4GA1UEBhMHVW5rbm93bjEQMA4G\n" +
        "A1UECBMHVW5rbm93bjEQMA4GA1UEBxMHVW5rbm93bjEQMA4GA1UEChMHY29kZS5kZTEQMA4GA1UE\n" +
        "CxMHVW5rbm93bjEQMA4GA1UEAxMHVW5rbm93bjAeFw0xNzA2MjMxNDE5MjBaFw0xNzA5MjExNDE5\n" +
        "MjBaMGwxEDAOBgNVBAYTB1Vua25vd24xEDAOBgNVBAgTB1Vua25vd24xEDAOBgNVBAcTB1Vua25v\n" +
        "d24xEDAOBgNVBAoTB2NvZGUuZGUxEDAOBgNVBAsTB1Vua25vd24xEDAOBgNVBAMTB1Vua25vd24w\n" +
        "ggG4MIIBLAYHKoZIzjgEATCCAR8CgYEA/X9TgR11EilS30qcLuzk5/YRt1I870QAwx4/gLZRJmlF\n" +
        "XUAiUftZPY1Y+r/F9bow9subVWzXgTuAHTRv8mZgt2uZUKWkn5/oBHsQIsJPu6nX/rfGG/g7V+fG\n" +
        "qKYVDwT7g/bTxR7DAjVUE1oWkTL2dfOuK2HXKu/yIgMZndFIAccCFQCXYFCPFSMLzLKSuYKi64QL\n" +
        "8Fgc9QKBgQD34aCF1ps93su8q1w2uFe5eZSvu/o66oL5V0wLPQeCZ1FZV4661FlP5nEHEIGAtEkW\n" +
        "cSPoTCgWE7fPCTKMyKbhPBZ6i1R8jSjgo64eK7OmdZFuo38L+iE1YvH7YnoBJDvMpPG+qFGQiaiD\n" +
        "3+Fa5Z8GkotmXoB7VSVkAUw7/s9JKgOBhQACgYEAz41IKVUQ13VqX2IqPUoTpmZ0ZBD+XJHQkuZh\n" +
        "Pxw6ZNkPlZbLdbAlC7vSHlC5d3FsOMbD7i1mFQ7KmaDYBb0rsivEw4uupboCu8Q8iBvxl1AjFtRp\n" +
        "+io5A3jjPBbhtt8C3ZFSdj0b/iFJwp9ub51pDjbkUbBXmrX3Sm4Momy/BzmjITAfMB0GA1UdDgQW\n" +
        "BBS+HuorU5c1yljoTigZ6SGeNP86oDALBgcqhkjOOAQDBQADLwAwLAIUHkZUNqCb6VjhxbW9vcRh\n" +
        "P8VfV8cCFClquisEfDHmF2u6Kl3OMWs4HLD9</ds:X509Certificate>\n" +
        "                </ds:X509Data>\n" +
        "            </ds:KeyInfo>\n" +
        "        </ds:Signature>\n" +
        "        <saml2:Subject>\n" +
        "            <saml2:NameID>cd_calvalus</saml2:NameID>\n" +
        "        </saml2:Subject>\n" +
        "        <saml2:Conditions NotBefore=\"2017-06-23T10:27:05.354Z\" NotOnOrAfter=\"2017-06-24T10:27:05.354Z\"/>\n" +
        "        <saml2:AuthnStatement>\n" +
        "            <saml2:AuthnContext>\n" +
        "                <saml2:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:Password</saml2:AuthnContextClassRef>\n" +
        "            </saml2:AuthnContext>\n" +
        "        </saml2:AuthnStatement>\n" +
        "        <saml2:AttributeStatement>\n" +
        "            <saml2:Attribute Name=\"groups\">\n" +
        "                <saml2:AttributeValue\n" +
        "                    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalus,testproject</saml2:AttributeValue>\n" +
        "            </saml2:Attribute>\n" +
        "            <saml2:Attribute Name=\"email\">\n" +
        "                <saml2:AttributeValue\n" +
        "                    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalustest@code.de</saml2:AttributeValue>\n" +
        "            </saml2:Attribute>\n" +
        "        </saml2:AttributeStatement>\n" +
        "    </saml2:Assertion>\n" +
        "</saml2p:Response>\n";

    public static final String HASH_AND_SAML_RESPONSE = "f1e596e8f7f28714d58447cf91189667704706337eab4b74e3722822b058cae3 <?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<saml2p:Response Version=\"2.0\" xmlns:saml2p=\"urn:oasis:names:tc:SAML:2.0:protocol\"><saml2:Assertion Version=\"2.0\" xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"><saml2:Issuer>cas</saml2:Issuer><ds:Signature xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\"><ds:SignedInfo><ds:CanonicalizationMethod Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/><ds:SignatureMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#dsa-sha1\"/><ds:Reference URI=\"\"><ds:Transforms><ds:Transform Algorithm=\"http://www.w3.org/2000/09/xmldsig#enveloped-signature\"/><ds:Transform Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"><ec:InclusiveNamespaces PrefixList=\"xs\" xmlns:ec=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/></ds:Transform></ds:Transforms><ds:DigestMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#sha1\"/><ds:DigestValue>jsFpcQ9z8tMHdtk0/oYwP8kAxBc=</ds:DigestValue></ds:Reference></ds:SignedInfo><ds:SignatureValue>TO3J92WEr5P1fWJpx538BPkPqECLuFo/mcYCuxIzHK5mnEqHEC44Uw==</ds:SignatureValue><ds:KeyInfo><ds:X509Data><ds:X509Certificate>MIIDNTCCAvOgAwIBAgIEYJZHtDALBgcqhkjOOAQDBQAwbDEQMA4GA1UEBhMHVW5rbm93bjEQMA4G\n" +
            "A1UECBMHVW5rbm93bjEQMA4GA1UEBxMHVW5rbm93bjEQMA4GA1UEChMHY29kZS5kZTEQMA4GA1UE\n" +
            "CxMHVW5rbm93bjEQMA4GA1UEAxMHVW5rbm93bjAeFw0xNzA2MjMxNDE5MjBaFw0xNzA5MjExNDE5\n" +
            "MjBaMGwxEDAOBgNVBAYTB1Vua25vd24xEDAOBgNVBAgTB1Vua25vd24xEDAOBgNVBAcTB1Vua25v\n" +
            "d24xEDAOBgNVBAoTB2NvZGUuZGUxEDAOBgNVBAsTB1Vua25vd24xEDAOBgNVBAMTB1Vua25vd24w\n" +
            "ggG4MIIBLAYHKoZIzjgEATCCAR8CgYEA/X9TgR11EilS30qcLuzk5/YRt1I870QAwx4/gLZRJmlF\n" +
            "XUAiUftZPY1Y+r/F9bow9subVWzXgTuAHTRv8mZgt2uZUKWkn5/oBHsQIsJPu6nX/rfGG/g7V+fG\n" +
            "qKYVDwT7g/bTxR7DAjVUE1oWkTL2dfOuK2HXKu/yIgMZndFIAccCFQCXYFCPFSMLzLKSuYKi64QL\n" +
            "8Fgc9QKBgQD34aCF1ps93su8q1w2uFe5eZSvu/o66oL5V0wLPQeCZ1FZV4661FlP5nEHEIGAtEkW\n" +
            "cSPoTCgWE7fPCTKMyKbhPBZ6i1R8jSjgo64eK7OmdZFuo38L+iE1YvH7YnoBJDvMpPG+qFGQiaiD\n" +
            "3+Fa5Z8GkotmXoB7VSVkAUw7/s9JKgOBhQACgYEAz41IKVUQ13VqX2IqPUoTpmZ0ZBD+XJHQkuZh\n" +
            "Pxw6ZNkPlZbLdbAlC7vSHlC5d3FsOMbD7i1mFQ7KmaDYBb0rsivEw4uupboCu8Q8iBvxl1AjFtRp\n" +
            "+io5A3jjPBbhtt8C3ZFSdj0b/iFJwp9ub51pDjbkUbBXmrX3Sm4Momy/BzmjITAfMB0GA1UdDgQW\n" +
            "BBS+HuorU5c1yljoTigZ6SGeNP86oDALBgcqhkjOOAQDBQADLwAwLAIUHkZUNqCb6VjhxbW9vcRh\n" +
            "P8VfV8cCFClquisEfDHmF2u6Kl3OMWs4HLD9</ds:X509Certificate></ds:X509Data></ds:KeyInfo></ds:Signature><saml2:Subject><saml2:NameID>cd_calvalus</saml2:NameID></saml2:Subject><saml2:Conditions NotBefore=\"2017-06-23T10:27:05.354Z\" NotOnOrAfter=\"2017-06-24T10:27:05.354Z\"/><saml2:AuthnStatement><saml2:AuthnContext><saml2:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:Password</saml2:AuthnContextClassRef></saml2:AuthnContext></saml2:AuthnStatement><saml2:AttributeStatement><saml2:Attribute Name=\"groups\"><saml2:AttributeValue xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalus,testproject</saml2:AttributeValue></saml2:Attribute><saml2:Attribute Name=\"email\"><saml2:AttributeValue xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalustest@code.de</saml2:AttributeValue></saml2:Attribute></saml2:AttributeStatement></saml2:Assertion></saml2p:Response>";

    @Test
    public void testUnencryptedSamlToken() throws Exception {
        Response response = createUnencryptedSamlToken();
        String formattedAssertion = util.pp(response);
        assertEquals("SAML token differs", SIMPLE_SAML_RESPONSE, formattedAssertion);
    }

    public Response createUnencryptedSamlToken() throws Exception {
        String issuer = "cas";
        String subject = "cd_calvalus";
        Map<String,String> attributes = new HashMap<>();
        attributes.put("groups", "calvalus,testproject");
        attributes.put("email", "calvalustest@code.de");
        int timeoutSeconds = 60*60*24;

        Response response = util.build(issuer, subject, attributes, new DateTime("2017-06-23T10:27:05.354Z"), timeoutSeconds);
        return response;
    }

    @Ignore
    @Test
    public void testSignedSamlToken() throws Exception {
        Response response = createSignedSamlToken();
        String formattedAssertion = util.pp(response);
        int p0 = formattedAssertion.indexOf("<ds:SignatureValue>");
        int p1 = formattedAssertion.indexOf("</ds:SignatureValue>");
        String comparableAssertion = formattedAssertion.substring(0, p0) + SIGNED_SAML_RESPONSE.substring(p0, p1) + formattedAssertion.substring(p1);
        assertEquals("SAML token differs", SIGNED_SAML_RESPONSE, comparableAssertion);
    }

    public Response createSignedSamlToken() throws Exception {
        Response response = createUnencryptedSamlToken();

        String certificateAliasName = "cas_certificate";
        String password = "secret";
        String keyStoreFileName = "/home/boe/tmp/code/caskeystore3.keys";
        Credential credentials = util.readCredentials(password, keyStoreFileName, certificateAliasName);

        response = util.sign(response, credentials);
        return response;
    }

    @Ignore
    @Test
    public void testHashAndSignedSamlToken() throws Exception {
        String hashAndSaml = createHashAndSignedSamlToken();
        int p0 = hashAndSaml.indexOf("<ds:SignatureValue>");
        int p1 = hashAndSaml.indexOf("</ds:SignatureValue>");
        String comparableHashAndSaml =
                hashAndSaml.substring(0, p0) + HASH_AND_SAML_RESPONSE.substring(p0, p1) + hashAndSaml.substring(p1);
        assertEquals("request digest and saml token", HASH_AND_SAML_RESPONSE, comparableHashAndSaml);
    }

    public String createHashAndSignedSamlToken() throws Exception {
        Response response = createSignedSamlToken();
        String responseString = util.toString(response);

        String request = "thisissomepayload";
        byte[] digest = util.sha256(request);
        String digestString = Hex.encodeHexString(digest);

        String hashAndSaml = digestString + ' ' + responseString;
        return hashAndSaml;
    }

    @Ignore
    @Test
    public void testEncryptedCalvalusToken() throws Exception {
        String calvalusToken = createCalvalusToken();

        assertTrue("key+hash+saml", calvalusToken.contains(" "));

        try (FileOutputStream out = new FileOutputStream("/home/boe/tmp/code/calvalus_token.dat")) {
            out.write(calvalusToken.getBytes());
        }
    }

    private String createCalvalusToken() throws Exception {
        String hashAndSaml = createHashAndSignedSamlToken();

        File keyFile = new File("/home/boe/tmp/code/calvalus_pub.der");
        byte[] keySequence = new byte[(int) keyFile.length()];
        try (FileInputStream in = new FileInputStream(keyFile)) {
            System.out.println(keyFile.length() + " ?= " + in.read(keySequence));
        }
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(keySequence);
        PublicKey rsaKey = keyFactory.generatePublic(publicKeySpec);

        return util.encryptRsaAes(hashAndSaml, rsaKey);
    }

    @Ignore
    @Test
    public void testDecryptedCalvalusToken() throws Exception {
        String hashAndSaml = decryptCalvalusToken();
        int p0 = hashAndSaml.indexOf("<ds:SignatureValue>");
        int p1 = hashAndSaml.indexOf("</ds:SignatureValue>");
        String comparableHashAndSaml =
                hashAndSaml.substring(0, p0) + HASH_AND_SAML_RESPONSE.substring(p0, p1) + hashAndSaml.substring(p1);
        assertEquals("request digest and saml token", HASH_AND_SAML_RESPONSE, comparableHashAndSaml);
    }

    String decryptCalvalusToken() throws Exception {
        String calvalusToken = createCalvalusToken();

        File keyFile = new File("/home/boe/tmp/code/calvalus_priv.der");
        byte[] keySequence = new byte[(int) keyFile.length()];
        try (FileInputStream in = new FileInputStream(keyFile)) {
            in.read(keySequence);
        }
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keySequence);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PrivateKey rsaKey = keyFactory.generatePrivate(keySpec);

        return util.decryptCalvalusToken(calvalusToken, rsaKey);
    }

    @Test
    public void encryptDecryptTest() throws Exception {
        Base64.Encoder encoder = Base64.getEncoder();
        Base64.Decoder decoder = Base64.getDecoder();

        KeyGenerator keygen1 = KeyGenerator.getInstance("AES");
        keygen1.init(128);
        Key key = keygen1.generateKey();
        byte[] aesKeyBytes = key.getEncoded();
        System.out.println(new String(encoder.encode(aesKeyBytes)));

//        KeyPairGenerator keygen = KeyPairGenerator.getInstance("RSA");
//        keygen.initialize(2048);
//        KeyPair rsaKeys = keygen.genKeyPair();
//        PrivateKey privateKey = rsaKeys.getPrivate();
//        PublicKey publicKey = rsaKeys.getPublic();
//
//        try (FileOutputStream out = new FileOutputStream("/home/boe/tmp/code/test_priv.der")) {
//            out.write(privateKey.getEncoded());
//        }
//        try (FileOutputStream out = new FileOutputStream("/home/boe/tmp/code/test_pub.der")) {
//            out.write(publicKey.getEncoded());
//        }

        KeyFactory keyFactory = KeyFactory.getInstance("RSA");

        byte[] privateKeyBytes = new byte[(int) new File("/home/boe/tmp/code/calvalus_priv.der").length()];
        try (FileInputStream in = new FileInputStream("/home/boe/tmp/code/calvalus_priv.der")) {
            System.out.println(new File("/home/boe/tmp/code/calvalus_priv.der").length() + " ?= " + in.read(privateKeyBytes));
        }
        byte[] publicKeyBytes = new byte[(int) new File("/home/boe/tmp/code/calvalus_pub.der").length()];
        try (FileInputStream in = new FileInputStream("/home/boe/tmp/code/calvalus_pub.der")) {
            System.out.println(new File("/home/boe/tmp/code/calvalus_pub.der").length() + " ?= " + in.read(publicKeyBytes));
        }

        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(privateKeyBytes);
        PrivateKey privateKey = keyFactory.generatePrivate(keySpec);
        X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(publicKeyBytes);
        PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);

        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.ENCRYPT_MODE, publicKey);
        byte[] encrypted = cipher.doFinal(aesKeyBytes);

        String encodedEncryptedAesKey = new String(encoder.encode(encrypted));
        System.out.println(encrypted.length + " " + encodedEncryptedAesKey);

        Cipher cipher2 = Cipher.getInstance("RSA");
        cipher2.init(Cipher.DECRYPT_MODE, privateKey);
        byte[] decrypted = cipher2.doFinal(decoder.decode(encodedEncryptedAesKey.getBytes()));

        System.out.println(new String(encoder.encode(decrypted)));
    }
}
