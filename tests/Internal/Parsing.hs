{-# LANGUAGE OverloadedStrings #-}

module Internal.Parsing where

import DataFrame.Internal.Parsing
import Test.HUnit

-- isNullish: recognized null strings

isNullishEmptyString :: Test
isNullishEmptyString =
    TestCase (assertBool "empty string is nullish" (isNullish ""))

isNullishNA :: Test
isNullishNA = TestCase (assertBool "NA is nullish" (isNullish "NA"))

isNullishNULL :: Test
isNullishNULL = TestCase (assertBool "NULL is nullish" (isNullish "NULL"))

isNullishNull :: Test
isNullishNull = TestCase (assertBool "null is nullish" (isNullish "null"))

isNullishNaN :: Test
isNullishNaN = TestCase (assertBool "nan is nullish" (isNullish "nan"))

isNullishNaNMixed :: Test
isNullishNaNMixed = TestCase (assertBool "NaN is nullish" (isNullish "NaN"))

isNullishNANUpper :: Test
isNullishNANUpper = TestCase (assertBool "NAN is nullish" (isNullish "NAN"))

isNullishNothing :: Test
isNullishNothing =
    TestCase (assertBool "Nothing is nullish" (isNullish "Nothing"))

isNullishSpace :: Test
isNullishSpace =
    TestCase (assertBool "single space is nullish" (isNullish " "))

isNullishNSlashA :: Test
isNullishNSlashA = TestCase (assertBool "N/A is nullish" (isNullish "N/A"))

-- isNullish: values that are NOT null

notNullishNumber :: Test
notNullishNumber =
    TestCase (assertBool "\"42\" is not nullish" (not (isNullish "42")))

notNullishText :: Test
notNullishText =
    TestCase (assertBool "\"hello\" is not nullish" (not (isNullish "hello")))

notNullishTrue :: Test
notNullishTrue =
    TestCase (assertBool "\"True\" is not nullish" (not (isNullish "True")))

notNullishDouble :: Test
notNullishDouble =
    TestCase (assertBool "\"3.14\" is not nullish" (not (isNullish "3.14")))

notNullishZero :: Test
notNullishZero =
    TestCase (assertBool "\"0\" is not nullish" (not (isNullish "0")))

-- readBool: positive cases

readBoolTrue :: Test
readBoolTrue =
    TestCase (assertEqual "readBool \"True\"" (Just True) (readBool "True"))

readBoolTrueLower :: Test
readBoolTrueLower =
    TestCase (assertEqual "readBool \"true\"" (Just True) (readBool "true"))

readBoolTrueUpper :: Test
readBoolTrueUpper =
    TestCase (assertEqual "readBool \"TRUE\"" (Just True) (readBool "TRUE"))

readBoolFalse :: Test
readBoolFalse =
    TestCase (assertEqual "readBool \"False\"" (Just False) (readBool "False"))

readBoolFalseLower :: Test
readBoolFalseLower =
    TestCase (assertEqual "readBool \"false\"" (Just False) (readBool "false"))

readBoolFalseUpper :: Test
readBoolFalseUpper =
    TestCase (assertEqual "readBool \"FALSE\"" (Just False) (readBool "FALSE"))

-- readBool: values that are not booleans

readBoolDigit :: Test
readBoolDigit =
    TestCase (assertEqual "readBool \"1\" is Nothing" Nothing (readBool "1"))

readBoolYes :: Test
readBoolYes =
    TestCase (assertEqual "readBool \"yes\" is Nothing" Nothing (readBool "yes"))

readBoolEmpty :: Test
readBoolEmpty =
    TestCase (assertEqual "readBool \"\" is Nothing" Nothing (readBool ""))

readBoolPartialTrue :: Test
readBoolPartialTrue =
    TestCase (assertEqual "readBool \"Tru\" is Nothing" Nothing (readBool "Tru"))

-- readInt

readIntPositive :: Test
readIntPositive =
    TestCase (assertEqual "readInt \"42\"" (Just 42) (readInt "42"))

readIntNegative :: Test
readIntNegative =
    TestCase (assertEqual "readInt \"-17\"" (Just (-17)) (readInt "-17"))

readIntZero :: Test
readIntZero =
    TestCase (assertEqual "readInt \"0\"" (Just 0) (readInt "0"))

-- readInt strips whitespace before parsing
readIntLeadingSpace :: Test
readIntLeadingSpace =
    TestCase
        ( assertEqual
            "readInt \" 5 \" (strips whitespace)"
            (Just 5)
            (readInt " 5 ")
        )

readIntFloat :: Test
readIntFloat =
    TestCase
        (assertEqual "readInt \"3.14\" is Nothing" Nothing (readInt "3.14"))

readIntText :: Test
readIntText =
    TestCase (assertEqual "readInt \"abc\" is Nothing" Nothing (readInt "abc"))

readIntEmpty :: Test
readIntEmpty =
    TestCase (assertEqual "readInt \"\" is Nothing" Nothing (readInt ""))

-- trailing non-digits must make the parse fail
readIntPartialSuffix :: Test
readIntPartialSuffix =
    TestCase
        ( assertEqual
            "readInt \"42abc\" is Nothing"
            Nothing
            (readInt "42abc")
        )

-- readDouble

readDoublePositive :: Test
readDoublePositive =
    TestCase
        (assertEqual "readDouble \"3.14\"" (Just 3.14) (readDouble "3.14"))

readDoubleNegative :: Test
readDoubleNegative =
    TestCase
        (assertEqual "readDouble \"-1.5\"" (Just (-1.5)) (readDouble "-1.5"))

readDoubleWholeNumber :: Test
readDoubleWholeNumber =
    TestCase
        ( assertEqual
            "readDouble \"42\" parses as 42.0"
            (Just 42.0)
            (readDouble "42")
        )

readDoubleText :: Test
readDoubleText =
    TestCase
        (assertEqual "readDouble \"abc\" is Nothing" Nothing (readDouble "abc"))

readDoubleEmpty :: Test
readDoubleEmpty =
    TestCase
        (assertEqual "readDouble \"\" is Nothing" Nothing (readDouble ""))

readDoublePartialSuffix :: Test
readDoublePartialSuffix =
    TestCase
        ( assertEqual
            "readDouble \"3.14abc\" is Nothing"
            Nothing
            (readDouble "3.14abc")
        )

tests :: [Test]
tests =
    [ TestLabel "isNullishEmptyString" isNullishEmptyString
    , TestLabel "isNullishNA" isNullishNA
    , TestLabel "isNullishNULL" isNullishNULL
    , TestLabel "isNullishNull" isNullishNull
    , TestLabel "isNullishNaN" isNullishNaN
    , TestLabel "isNullishNaNMixed" isNullishNaNMixed
    , TestLabel "isNullishNANUpper" isNullishNANUpper
    , TestLabel "isNullishNothing" isNullishNothing
    , TestLabel "isNullishSpace" isNullishSpace
    , TestLabel "isNullishNSlashA" isNullishNSlashA
    , TestLabel "notNullishNumber" notNullishNumber
    , TestLabel "notNullishText" notNullishText
    , TestLabel "notNullishTrue" notNullishTrue
    , TestLabel "notNullishDouble" notNullishDouble
    , TestLabel "notNullishZero" notNullishZero
    , TestLabel "readBoolTrue" readBoolTrue
    , TestLabel "readBoolTrueLower" readBoolTrueLower
    , TestLabel "readBoolTrueUpper" readBoolTrueUpper
    , TestLabel "readBoolFalse" readBoolFalse
    , TestLabel "readBoolFalseLower" readBoolFalseLower
    , TestLabel "readBoolFalseUpper" readBoolFalseUpper
    , TestLabel "readBoolDigit" readBoolDigit
    , TestLabel "readBoolYes" readBoolYes
    , TestLabel "readBoolEmpty" readBoolEmpty
    , TestLabel "readBoolPartialTrue" readBoolPartialTrue
    , TestLabel "readIntPositive" readIntPositive
    , TestLabel "readIntNegative" readIntNegative
    , TestLabel "readIntZero" readIntZero
    , TestLabel "readIntLeadingSpace" readIntLeadingSpace
    , TestLabel "readIntFloat" readIntFloat
    , TestLabel "readIntText" readIntText
    , TestLabel "readIntEmpty" readIntEmpty
    , TestLabel "readIntPartialSuffix" readIntPartialSuffix
    , TestLabel "readDoublePositive" readDoublePositive
    , TestLabel "readDoubleNegative" readDoubleNegative
    , TestLabel "readDoubleWholeNumber" readDoubleWholeNumber
    , TestLabel "readDoubleText" readDoubleText
    , TestLabel "readDoubleEmpty" readDoubleEmpty
    , TestLabel "readDoublePartialSuffix" readDoublePartialSuffix
    ]
