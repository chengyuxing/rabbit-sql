/*{fields}*/
id, name, address, ${otherFields}, '${hobby}';

/*{otherFields}*/
id_card, enable, ${hobby};

/*{hobby}*/
reading, swiming, computer_game, girl;

/*[getUser]*/
select ${fields}, ${concats}, '${otherFields}' from ${db}.user where id = :id;

/*{concats}*/
email, phone, message, facetime;

/*[getGuest]*/
select ${hobby} from test.user;