# Problem 2 - Decomposition

Consider the following table:

|fourm_name|popularity|post_id|title|article|reply|
|---|---|---|---|---|---|
|Gossiping|100|131|Girlfriend|How can I get girlfriend ?|["Haha", "I don't know"]
|Gossiping|100|252|Firends|I don't have a friend...|["Haha", "I can be", "QQ"]
|Joke|23|46|Knock|Knock! Knock! ...|["Then?", "What's the point ?"]
|Joke|23|151|Santa Claus|Hold! Hold! Hold!|["XDD"]

This table is not in the 3rd normal form.

Here are its funtional dependencies:
- fourm_name -> popularity
- {fourm_name, post_id} -> {title, article, reply}

Please decompose the table in order to make it **follow the 3rd normal form and identify which fields should be primary keys**. You must also keep the data in the table. You may add new fields to preserve the above relationships. (40 points)
