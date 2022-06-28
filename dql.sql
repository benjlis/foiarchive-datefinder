-- name: get-ids
select doc_id from foiarchive.un_archives_docs
    where body is not null
    order by doc_id;

-- name: get-head-body
select substr(body, 1, 512) from foiarchive.un_archives_docs
    where doc_id = :doc_id;
