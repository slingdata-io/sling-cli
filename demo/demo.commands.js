
async function run(input, parameters) {
  
  var se = Application('System Events');
  
  let commands = [
    {delay: 10},
    {text: 'sling conns list'},
    {delay: 3},
    {text: "export my_pg='postgresql://postgres:postgres@mpc:55432/postgres?sslmode=disable'"},
    {delay: 2},
    {text: 'sling conns list'},
    {delay: 3},
    {text: 'sling conns test my_pg'},
    {delay: 2},
    {text: 'sling conns discover my_pg --schema postgres'},
    {delay: 4},
    {text: "sling run --src-conn my_pg --src-stream 'select * from postgres.accounts where rating > 50 limit 10' --stdout"},
    {delay: 5},
    {text: "sling run --src-conn my_pg --src-stream 'postgres.accounts' --tgt-conn clickhouse --tgt-object default.accounts --mode full-refresh"},
    {delay: 13},
    {text: "export my_duck='duckdb://./duck.db'"},
    {delay: 2},
    {text: "sling conns list"},
    {delay: 3},
    {text: "sling run --src-conn clickhouse --src-stream 'default.accounts' --tgt-conn my_duck --tgt-object main.accounts"},
    {delay: 16},
    {text: "sling run --src-conn my_pg --src-stream 'select * from postgres.accounts where rating > 50' --tgt-conn my_duck --tgt-object main.accounts_prime"},
    {delay: 5},
    {text: "sling conns discover my_duck"},
  ];
    

  // https://eastmanreference.com/complete-list-of-applescript-key-codes
  
  for(let command of commands) {
    if(command.delay) {
      delay(command.delay)
      continue
    }

    for(let c of command.text) {
      se.keystroke(c)

      delay(0.1 * Math.random())
    }
    se.keyCode(36)
  }

  return input;
}