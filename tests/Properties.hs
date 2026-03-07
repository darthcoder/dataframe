module Properties where

import qualified DataFrame as D
import DataFrame.Internal.DataFrame (DataFrame, dataframeDimensions)

-- | distinct never increases row count.
prop_distinctMonotone :: DataFrame -> Bool
prop_distinctMonotone df =
    fst (dataframeDimensions (D.distinct df)) <= fst (dataframeDimensions df)

-- | distinct is idempotent.
prop_distinctIdempotent :: DataFrame -> Bool
prop_distinctIdempotent df = D.distinct (D.distinct df) == D.distinct df

-- | Merging empty on the left is identity for row count.
prop_mergeEmptyLeft :: DataFrame -> Bool
prop_mergeEmptyLeft df =
    fst (dataframeDimensions (D.empty <> df)) == fst (dataframeDimensions df)

-- | Merging empty on the right is identity for row count.
prop_mergeEmptyRight :: DataFrame -> Bool
prop_mergeEmptyRight df =
    fst (dataframeDimensions (df <> D.empty)) == fst (dataframeDimensions df)

-- | Merging a DataFrame with itself doubles the row count.
prop_mergeSelfDoubles :: DataFrame -> Bool
prop_mergeSelfDoubles df =
    fst (dataframeDimensions (df <> df)) == 2 * fst (dataframeDimensions df)

tests :: [DataFrame -> Bool]
tests =
    [ prop_distinctMonotone
    , prop_distinctIdempotent
    , prop_mergeEmptyLeft
    , prop_mergeEmptyRight
    , prop_mergeSelfDoubles
    ]
