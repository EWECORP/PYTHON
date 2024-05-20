# Rock Paper Scissors

This is the boilerplate for the Rock Paper Scissors project. Instructions for building your project can be found at https://www.freecodecamp.org/learn/machine-learning-with-python/machine-learning-with-python-projects/rock-paper-scissors


# Piedra Papel tijeras
Para este desafío, crearás un programa para jugar Piedra, Papel, Tijera. Un programa que elige al azar normalmente ganará el 50% de las veces. Para superar este desafío, tu programa debe jugar partidas contra cuatro bots diferentes, ganando al menos el 60% de las partidas en cada partida.

Trabajará en este proyecto con nuestro código de inicio de Gitpod .

Todavía estamos desarrollando la parte instructiva interactiva del plan de estudios de aprendizaje automático. Por ahora, tendrás que utilizar otros recursos para aprender a superar este desafío.

En el archivo **RPS.pyse** le proporciona una función llamada player. La función toma un argumento que es una cadena que describe el último movimiento del oponente ("R", "P" o "S"). La función debe devolver una cadena que represente el siguiente movimiento a realizar ("R", "P" o "S").

Una función de jugador recibirá una cadena vacía como argumento para el primer juego de un partido, ya que no hay ningún juego anterior.

El archivo **RPS.py** muestra una función de ejemplo que deberá actualizar. La función de ejemplo se define con dos argumentos ( player(prev_play, opponent_history = [])). La función nunca se llama con un segundo argumento, por lo que uno es completamente opcional. La razón por la cual la función de ejemplo contiene un segundo argumento ( opponent_history = []) es porque esa es la única forma de guardar el estado entre llamadas consecutivas de la playerfunción. Sólo necesitas el opponent_historyargumento si quieres realizar un seguimiento del historial_del_oponente.

*Sugerencia: Para derrotar a los cuatro oponentes, es posible que su programa necesite tener múltiples estrategias que cambien según las jugadas del oponente.*

# Desarrollo
No modificar **RPS_game.py**. Escribe todo tu código en **RPS.py**. Para el desarrollo, puede utilizar main.pypara probar su código.

**main.py** importa la función del juego y los bots de **RPS_game.py**.

Para probar su código, juegue un juego con la playfunción. La playfunción toma cuatro argumentos:

dos jugadores para jugar uno contra el otro (los jugadores en realidad son funciones)
el número de juegos a jugar en el partido
un argumento opcional para ver un registro de cada juego. Configúrelo para Truever estos mensajes.
play(player1, player2, num_games[, verbose])
Por ejemplo, así es como llamarías a la función si quieres playerjugar quincy1000 juegos entre sí y quieres ver los resultados de cada juego:

play(player, quincy, 1000, verbose=True)
# Pruebas
Las pruebas unitarias para este proyecto están en formato test_module.py. Importamos las pruebas de **test_module.py** a **main.py** para su comodidad. Si descomentas la última línea en **main.py**, las pruebas se ejecutarán automáticamente cada vez que las ejecutes **python main.py** en la consola.

Sumisión
Copie la URL de su proyecto y envíela a freeCodeCamp.


