ruby count_in_links1.rb --rm --run=hadoop /data/rawd/wp/linkgraph/a_linksto_b link_count1
ruby count_in_links2.rb --rm --run=hadoop link_count1/*,/data/rawd/wp/article_info/article_titles link_count
