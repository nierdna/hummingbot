import logging
import os
from decimal import Decimal
from typing import Dict

from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class RangeBoundConfig(BaseClientModel):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    connector: str = Field("jupiter", json_schema_extra={
        "prompt": "Connector name (e.g. jupiter, uniswap)", "prompt_on_new": True})
    chain: str = Field("solana", json_schema_extra={
        "prompt": "Chain (e.g. solana, ethereum)", "prompt_on_new": True})
    network: str = Field("mainnet-beta", json_schema_extra={
        "prompt": "Network (e.g. mainnet-beta (solana), mainnet (ethereum))", "prompt_on_new": True})
    trading_pair: str = Field("SOL-USDC", json_schema_extra={
        "prompt": "Trading pair (e.g. SOL-USDC)", "prompt_on_new": True})
    lower_bound: Decimal = Field(Decimal("20"), json_schema_extra={
        "prompt": "Lower price bound (buy when price falls below this)", "prompt_on_new": True})
    upper_bound: Decimal = Field(Decimal("25"), json_schema_extra={
        "prompt": "Upper price bound (sell when price rises above this)", "prompt_on_new": True})
    base_amount: Decimal = Field(Decimal("0.1"), json_schema_extra={
        "prompt": "Amount of base token to buy/sell per trade", "prompt_on_new": True})
    quote_amount: Decimal = Field(Decimal("2.5"), json_schema_extra={
        "prompt": "Amount of quote token to use for buying", "prompt_on_new": True})
    cooldown_time: int = Field(300, json_schema_extra={
        "prompt": "Cooldown time between trades in seconds", "prompt_on_new": True})
    max_trades: int = Field(10, json_schema_extra={
        "prompt": "Maximum number of trades to execute (0 for unlimited)", "prompt_on_new": True})


class RangeBoundAMM(ScriptStrategyBase):
    """
    This strategy keeps a token's price within a specific range on an AMM:
    - Buys the base token when price falls below lower bound
    - Sells the base token when price rises above upper bound
    
    This helps to stabilize token price while generating profit from volatility.
    """

    @classmethod
    def init_markets(cls, config: RangeBoundConfig):
        connector_chain_network = f"{config.connector}_{config.chain}_{config.network}"
        cls.markets = {connector_chain_network: {config.trading_pair}}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: RangeBoundConfig):
        super().__init__(connectors)
        self.config = config
        self.exchange = f"{config.connector}_{config.chain}_{config.network}"
        self.base, self.quote = self.config.trading_pair.split("-")

        # State tracking
        self.last_trade_timestamp = 0
        self.buy_trades_executed = 0
        self.sell_trades_executed = 0
        self.trade_in_progress = False
        self.total_trades_executed = 0

        # Log strategy information
        self.log_with_clock(
            logging.INFO, 
            f"Range Bound AMM strategy initialized on {self.exchange} for {self.config.trading_pair}\n"
            f"Price Range: {self.config.lower_bound} to {self.config.upper_bound} {self.quote}\n"
            f"Will buy {self.config.base_amount} {self.base} when price falls below {self.config.lower_bound}\n"
            f"Will sell {self.config.base_amount} {self.base} when price rises above {self.config.upper_bound}\n"
            f"Cooldown between trades: {self.config.cooldown_time} seconds\n"
            f"Max trades: {self.config.max_trades if self.config.max_trades > 0 else 'Unlimited'}"
        )

    def on_tick(self):
        # Check if max trades limit has been reached
        if self.config.max_trades > 0 and self.total_trades_executed >= self.config.max_trades:
            return

        # Don't check price if trade is in progress
        if self.trade_in_progress:
            return

        # Check price on each tick
        safe_ensure_future(self.check_price_and_trade())

    async def check_price_and_trade(self):
        """Check current price and trigger trade if conditions are met"""
        if self.trade_in_progress:
            return

        # Check cooldown period
        current_timestamp = self.current_timestamp
        if current_timestamp - self.last_trade_timestamp < self.config.cooldown_time:
            return

        self.trade_in_progress = True
        current_price = None

        try:
            # Get current market price
            self.log_with_clock(logging.INFO, f"Checking current price on {self.exchange} for {self.config.trading_pair}")
            current_price = await self.connectors[self.exchange].get_quote_price(
                trading_pair=self.config.trading_pair,
                is_buy=True,  # Just to get the price, we'll decide buy/sell later
                amount=self.config.base_amount,
            )
            self.log_with_clock(logging.INFO, f"Current price: {current_price} {self.quote}")
        except Exception as e:
            self.log_with_clock(logging.ERROR, f"Error getting price: {e}")
            self.trade_in_progress = False
            return

        # Continue only if we have a valid price
        if current_price is not None:
            # Determine if we should buy or sell
            should_buy = current_price < self.config.lower_bound
            should_sell = current_price > self.config.upper_bound

            if should_buy:
                await self.execute_buy(current_price)
            elif should_sell:
                await self.execute_sell(current_price)
            else:
                self.log_with_clock(
                    logging.INFO, 
                    f"Price is within range ({self.config.lower_bound} - {self.config.upper_bound}), no action needed."
                )
                self.trade_in_progress = False

    async def execute_buy(self, current_price):
        """Execute a buy order when price falls below lower bound"""
        try:
            self.log_with_clock(
                logging.INFO, 
                f"Price ({current_price}) fell below lower bound ({self.config.lower_bound}). Executing BUY..."
            )
            
            # Calculate buy amount based on available quote balance
            buy_amount = self.config.base_amount
            
            # Xử lý trading_pair dạng TOKEN-WETH hoặc TOKEN-SOL
            token_parts = self.config.trading_pair.split("-")
            if len(token_parts) == 2 and ("WETH" in token_parts or "SOL" in token_parts):
                main_token = "WETH" if "WETH" in token_parts else "SOL"
                # Đảo vị trí để WETH/SOL thành token đầu tiên
                reversed_trading_pair = "-".join([token_parts[1], token_parts[0]]) if token_parts[1] == main_token else "-".join([token_parts[0], token_parts[1]])
                
                self.log_with_clock(
                    logging.INFO, 
                    f"Reversing trading pair from {self.config.trading_pair} to {reversed_trading_pair} for buying tokens"
                )
                
                # Execute the buy order (thực chất là bán WETH/SOL để mua token)
                order_id = self.connectors[self.exchange].place_order(
                    is_buy=False,  # False vì bán WETH/SOL để mua token
                    trading_pair=reversed_trading_pair,
                    amount=buy_amount,
                    price=current_price,
                )
            else:
                # Nếu không phải dạng TOKEN_WETH hoặc TOKEN_SOL, sử dụng logic cũ
                order_id = self.connectors[self.exchange].place_order(
                    is_buy=True,
                    trading_pair=self.config.trading_pair,
                    amount=buy_amount,
                    price=current_price,
                )
            
            self.log_with_clock(logging.INFO, f"Buy order executed with order ID: {order_id}")
            self.buy_trades_executed += 1
            self.total_trades_executed += 1
            self.last_trade_timestamp = self.current_timestamp
        except Exception as e:
            self.log_with_clock(logging.ERROR, f"Error executing buy order: {str(e)}")
        finally:
            self.trade_in_progress = False

    async def execute_sell(self, current_price):
        """Execute a sell order when price rises above upper bound"""
        try:
            # Check available balance first
            available_balance = self.connectors[self.exchange].get_available_balance(self.base)
            
            if available_balance <= Decimal("0"):
                self.log_with_clock(
                    logging.INFO,
                    f"Price ({current_price}) rose above upper bound ({self.config.upper_bound}), but no {self.base} balance available to sell. Skipping..."
                )
                return
                
            self.log_with_clock(
                logging.INFO, 
                f"Price ({current_price}) rose above upper bound ({self.config.upper_bound}). Executing SELL of all available {self.base} ({available_balance})..."
            )
            
            # Execute the sell order with all available balance
            order_id = self.connectors[self.exchange].place_order(
                is_buy=False,
                trading_pair=self.config.trading_pair,
                amount=available_balance,
                price=current_price,
            )
            
            self.log_with_clock(logging.INFO, f"Sell order executed with order ID: {order_id}, amount: {available_balance} {self.base}")
            self.sell_trades_executed += 1
            self.total_trades_executed += 1
            self.last_trade_timestamp = self.current_timestamp
        except Exception as e:
            self.log_with_clock(logging.ERROR, f"Error executing sell order: {str(e)}")
        finally:
            self.trade_in_progress = False

    def format_status(self) -> str:
        """Format status message for display in Hummingbot"""
        lines = []
        connector_chain_network = f"{self.config.connector}_{self.config.chain}_{self.config.network}"
        
        lines.append(f"Range Bound AMM - {self.base}-{self.quote} on {connector_chain_network}")
        lines.append(f"Price Range: {self.config.lower_bound} to {self.config.upper_bound} {self.quote}")
        lines.append(f"Buy below: {self.config.lower_bound} | Sell above: {self.config.upper_bound}")
        lines.append(f"Base amount per trade: {self.config.base_amount} {self.base}")
        
        if self.config.max_trades > 0:
            lines.append(f"Trades executed: {self.total_trades_executed}/{self.config.max_trades}")
        else:
            lines.append(f"Trades executed: {self.total_trades_executed} (unlimited)")
            
        lines.append(f"Buy trades: {self.buy_trades_executed} | Sell trades: {self.sell_trades_executed}")
        
        # Add cooldown information
        current_timestamp = self.current_timestamp
        time_since_last_trade = current_timestamp - self.last_trade_timestamp
        if self.last_trade_timestamp > 0:
            if time_since_last_trade < self.config.cooldown_time:
                cooldown_remaining = self.config.cooldown_time - time_since_last_trade
                lines.append(f"Cooldown: {int(cooldown_remaining)} seconds remaining")
            else:
                lines.append(f"Ready to trade (cooldown period passed)")
        else:
            lines.append(f"No trades executed yet")

        return "\n".join(lines) 