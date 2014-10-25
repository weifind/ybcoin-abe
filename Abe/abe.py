#!/usr/bin/env python
# Copyright(C) 2011,2012 by John Tobey <John.Tobey@gmail.com>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public
# License along with this program.  If not, see
# <http://www.gnu.org/licenses/agpl.html>.

import sys
import os
import optparse
import re
from cgi import escape
import posixpath
import wsgiref.util
import time
import logging
import json

import version
import DataStore
import readconf

# bitcointools -- modified deserialize.py to return raw transaction
import deserialize
import util  # Added functions.
import base58

__version__ = version.__version__

ABE_APPNAME = "Ybcoin"
ABE_VERSION = __version__
ABE_URL = 'https://github.com/ybcoin/ybcoin-abe'

COPYRIGHT_YEARS = '2014'
COPYRIGHT = "Ybcoin"
COPYRIGHT_URL = "mailto:ifind@live.cn"

DONATIONS_BTC = ''
DONATIONS_YBC = 'YTU8JJidCcHtJpYMGPK2eL6zGBVKwd2Jit'

# Abe-generated content should all be valid HTML and XHTML fragments.
# Configurable templates may contain either.  HTML seems better supported
# under Internet Explorer.
# <p><a href="/static/graphs.htm">Ybcoin 统计数据</a></p>
DEFAULT_CONTENT_TYPE = "text/html; charset=utf-8"
DEFAULT_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <link rel="stylesheet" type="text/css"
     href="%(dotdot)s%(STATIC_PATH)sabe.css" />
    <link rel="shortcut icon" href="%(dotdot)s%(STATIC_PATH)sfavicon.ico" />
    <title>%(title)s</title>
</head>
<body>
    <h1><a href="%(dotdot)schains"><img
     src="%(dotdot)s%(STATIC_PATH)slogo32.png" alt="Abe logo" /></a> %(h1)s
    </h1>
    %(body)s
    <p><a href="%(dotdot)sq">API</a> 机读格式</p>
    <p style="font-size: smaller">
        <span style="font-style: italic">
            由 <a href="%(ABE_URL)s">Ybcoin-abe</a> 提供技术支持
        </span>
        %(download)s
        , 需要您的捐助
        <!-- <a href="%(dotdot)saddress/%(DONATIONS_BTC)s">BTC</a> -->
        <a href="%(dotdot)saddress/%(DONATIONS_YBC)s">YBC</a>
    	<script src="http://s11.cnzz.com/stat.php?id=5570768&web_id=5570768&show=pic1" language="JavaScript"></script>
    </p>
</body>
</html>
"""

DEFAULT_LOG_FORMAT = "%(message)s"

# XXX This should probably be a property of chain, or even a query param.
LOG10COIN = 6
COIN = 10 ** LOG10COIN

# It is fun to change "6" to "3" and search lots of addresses.
ADDR_PREFIX_RE = re.compile('[1-9A-HJ-NP-Za-km-z]{6,}\\Z')
HEIGHT_RE = re.compile('(?:0|[1-9][0-9]*)\\Z')
HASH_PREFIX_RE = re.compile('[0-9a-fA-F]{0,64}\\Z')
HASH_PREFIX_MIN = 6

NETHASH_HEADER = """\
blockNumber:          height of last block in interval + 1
time:                 block time in seconds since 0h00 1 Jan 1970 UTC
target:               decimal target at blockNumber
avgTargetSinceLast:   harmonic mean of target over interval
difficulty:           difficulty at blockNumber
hashesToWin:          expected number of hashes needed to solve a block at this difficulty
avgIntervalSinceLast: interval seconds divided by blocks
netHashPerSecond:     estimated network hash rate over interval

Statistical values are approximate.

/chain/CHAIN/q/nethash[/INTERVAL[/START[/STOP]]]
Default INTERVAL=144, START=0, STOP=infinity.
Negative values back from the last block.

blockNumber,time,target,avgTargetSinceLast,difficulty,hashesToWin,avgIntervalSinceLast,netHashPerSecond
START DATA
"""

# How many addresses to accept in /unspent/ADDR|ADDR|...
MAX_UNSPENT_ADDRESSES = 200

def make_store(args):
    store = DataStore.new(args)
    store.catch_up()
    return store

class NoSuchChainError(Exception):
    """Thrown when a chain lookup fails"""

class PageNotFound(Exception):
    """Thrown when code wants to return 404 Not Found"""

class Redirect(Exception):
    """Thrown when code wants to redirect the request"""

class Streamed(Exception):
    """Thrown when code has written the document to the callable
    returned by start_response."""

class Abe:
    def __init__(abe, store, args):
        abe.store = store
        abe.args = args
        abe.htdocs = args.document_root or find_htdocs()
        abe.static_path = '' if args.static_path is None else args.static_path
        abe.template_vars = args.template_vars.copy()
        abe.template_vars['STATIC_PATH'] = (
            abe.template_vars.get('STATIC_PATH', abe.static_path))
        abe.template = flatten(args.template)
        abe.debug = args.debug
        abe.log = logging.getLogger(__name__)
        abe.log.info('Abe initialized.')
        abe.home = "chains"
        if not args.auto_agpl:
            abe.template_vars['download'] = (
                abe.template_vars.get('download', ''))
        abe.base_url = args.base_url
        abe.address_history_rows_max = int(
            args.address_history_rows_max or 100000)

        if args.shortlink_type is None:
            abe.shortlink_type = ("firstbits" if store.use_firstbits else
                                  "non-firstbits")
        else:
            abe.shortlink_type = args.shortlink_type
            if abe.shortlink_type != "firstbits":
                abe.shortlink_type = int(abe.shortlink_type)
                if abe.shortlink_type < 2:
                    raise ValueError("shortlink-type: 2 character minimum")
            elif not store.use_firstbits:
                abe.shortlink_type = "non-firstbits"
                abe.log.warn("Ignoring shortlink-type=firstbits since" +
                             " the database does not support it.")
        if abe.shortlink_type == "non-firstbits":
            abe.shortlink_type = 10

    def __call__(abe, env, start_response):
        import urlparse

        status = '200 OK'
        page = {
            "title": [escape(ABE_APPNAME), " ", ABE_VERSION],
            "body": [],
            "env": env,
            "params": {},
            "dotdot": "../" * (env['PATH_INFO'].count('/') - 1),
            "start_response": start_response,
            "content_type": str(abe.template_vars['CONTENT_TYPE']),
            "template": abe.template,
            "chain": None,
            }
        if 'QUERY_STRING' in env:
            page['params'] = urlparse.parse_qs(env['QUERY_STRING'])

        if fix_path_info(env):
            abe.log.debug("fixed path_info")
            return redirect(page)

        cmd = wsgiref.util.shift_path_info(env)
        if cmd == '':
            cmd = abe.home
        handler = abe.get_handler(cmd)        
        try:
            if handler is None:
                return abe.serve_static(cmd + env['PATH_INFO'], start_response)

            # Always be up-to-date, even if we means having to wait
            # for a response!  XXX Could use threads, timers, or a
            # cron job.
            abe.store.catch_up()

            handler(page)
        except PageNotFound:
            status = '404 Not Found'
            page["body"] = ['<p class="error">Sorry, ', env['SCRIPT_NAME'],
                            env['PATH_INFO'],
                            ' does not exist on this server.</p>']
        except NoSuchChainError, e:
            page['body'] += [
                '<p class="error">'
                'Sorry, I don\'t know about that chain!</p>\n']
        except Redirect:
            return redirect(page)
        except Streamed:
            return ''
        except:
            abe.store.rollback()
            raise

        abe.store.rollback()  # Close imlicitly opened transaction.

        start_response(status, [('Content-type', page['content_type']),
                                ('Cache-Control', 'max-age=30')])

        tvars = abe.template_vars.copy()
        tvars['dotdot'] = page['dotdot']
        tvars['title'] = flatten(page['title'])
        tvars['h1'] = flatten(page.get('h1') or page['title'])
        tvars['body'] = flatten(page['body'])
        if abe.args.auto_agpl:
            tvars['download'] = (
                ' <a href="' + page['dotdot'] + 'download">Source</a>')

        content = page['template'] % tvars
        if isinstance(content, unicode):
            content = content.encode('UTF-8')
        return content

    def get_handler(abe, cmd):
        return getattr(abe, 'handle_' + cmd, None)

    def handle_chains(abe, page):
        page['title'] = ABE_APPNAME + ' Explorer'
        body = page['body']
        body += [
            abe.search_form(page),
            '<table>\n',
            '<tr><th>货币</th><th>代号</th><th>区块</th><th>时间(GMT)</th>',
            '<th>开始时间</th><th>币龄</th><th>已发行</th>',
            '<th>平均币龄</th><th>',
            '% <a href="https://en.bitcoin.it/wiki/Bitcoin_Days_Destroyed">',
            'CoinDD</a></th>',
            '</tr>\n']
        now = time.time()

        rows = abe.store.selectall("""
            SELECT c.chain_name, b.block_height, (b.block_nTime+28800), b.block_hash,
                   b.block_total_seconds, b.block_total_satoshis,
                   b.block_satoshi_seconds,
                   b.block_total_ss, c.chain_id, c.chain_code3,
                   c.chain_address_version, c.chain_last_block_id
              FROM chain c
              JOIN block b ON (c.chain_last_block_id = b.block_id)
             ORDER BY c.chain_name
        """)
        for row in rows:
            name = row[0]
            chain = abe._row_to_chain((row[8], name, row[9], row[10], row[11]))
            body += [
                '<tr><td><a href="chain/', escape(name), '">',
                escape(name), '</a></td><td>', escape(chain['code3']), '</td>']

            if row[1] is not None:
                (height, nTime, hash) = (
                    int(row[1]), int(row[2]), abe.store.hashout_hex(row[3]))

                body += [
                    '<td><a href="block/', hash, '">', height, '</a></td>',
                    '<td>', format_time(nTime), '</td>']

                #if row[6] is not None and row[7] is not None:
                (seconds, satoshis, ss, total_ss) = (
                    int(row[4]), int(row[5]), int(row[6] or 0), int(row[7] or 0))

                started = nTime - seconds
                chain_age = now - started
                since_block = now - nTime

                if satoshis == 0 or satoshis == -1:
                    avg_age = '&nbsp;'
                    satoshis = 0;
                else:
                    avg_age = '%5g' % ((float(ss) / satoshis + since_block)
                                       / 86400.0)

                if chain_age <= 0 or total_ss <= 0:
                    percent_destroyed = '&nbsp;'
                else:
                    more = since_block * satoshis
                    percent_destroyed = '%5g' % (
                        100.0 - (100.0 * (ss + more) / (total_ss + more)))
                    percent_destroyed += '%'

                body += [
                    '<td>', format_time(started)[:10], '</td>',
                    '<td>', '%5g' % (chain_age / 86400.0), '</td>',
                    '<td>', format_satoshis(satoshis, chain), '</td>',
                    '<td>', avg_age, '</td>',
                    '<td>', percent_destroyed, '</td>']

            body += ['</tr>\n']
        body += ['</table>\n']
        if len(rows) == 0:
            body += ['<p>No block data found.</p>\n']

    def _chain_fields(abe):
        return ["id", "name", "code3", "address_version", "last_block_id"]

    def _row_to_chain(abe, row):
        if row is None:
            raise NoSuchChainError()
        chain = {}
        fields = abe._chain_fields()
        for i in range(len(fields)):
            chain[fields[i]] = row[i]
        chain['address_version'] = abe.store.binout(chain['address_version'])
        return chain

    def chain_lookup_by_name(abe, symbol):
        if symbol is None:
            return abe.get_default_chain()
        return abe._row_to_chain(abe.store.selectrow("""
            SELECT chain_""" + ", chain_".join(abe._chain_fields()) + """
              FROM chain
             WHERE chain_name = ?""", (symbol,)))

    def get_default_chain(abe):
        return abe.chain_lookup_by_name('Bitcoin')

    def chain_lookup_by_id(abe, chain_id):
        return abe._row_to_chain(abe.store.selectrow("""
            SELECT chain_""" + ", chain_".join(abe._chain_fields()) + """
              FROM chain
             WHERE chain_id = ?""", (chain_id,)))

    def call_handler(abe, page, cmd):
        handler = abe.get_handler(cmd)
        if handler is None:
            raise PageNotFound()
        handler(page)

    def handle_chain(abe, page):
        symbol = wsgiref.util.shift_path_info(page['env'])
        chain = abe.chain_lookup_by_name(symbol)
        page['chain'] = chain

        cmd = wsgiref.util.shift_path_info(page['env'])
        if cmd == '':
            page['env']['SCRIPT_NAME'] = page['env']['SCRIPT_NAME'][:-1]
            raise Redirect()
        if cmd == 'chain' or cmd == 'chains':
            raise PageNotFound()
        if cmd is not None:
            abe.call_handler(page, cmd)
            return

        page['title'] = chain['name']

        body = page['body']
        body += abe.search_form(page)

        count = get_int_param(page, 'count') or 20
        hi = get_int_param(page, 'hi')
        orig_hi = hi

        if hi is None:
            row = abe.store.selectrow("""
                SELECT b.block_height
                  FROM block b
                  JOIN chain c ON (c.chain_last_block_id = b.block_id)
                 WHERE c.chain_id = ?
            """, (chain['id'],))
            if row:
                hi = row[0]
        if hi is None:
            if orig_hi is None and count > 0:
                body += ['<p>I have no blocks in this chain.</p>']
            else:
                body += ['<p class="error">'
                         'The requested range contains no blocks.</p>\n']
            return

        rows = abe.store.selectall("""
            SELECT b.block_hash, b.block_height, (b.block_nTime+28800), b.block_num_tx,
                   b.block_nBits, b.block_value_out,
                   b.block_total_seconds, b.block_satoshi_seconds,
                   b.block_total_satoshis, b.block_ss_destroyed,
                   b.block_total_ss
              FROM block b
              JOIN chain_candidate cc ON (b.block_id = cc.block_id)
             WHERE cc.chain_id = ?
               AND cc.block_height BETWEEN ? AND ?
               AND cc.in_longest = 1
             ORDER BY cc.block_height DESC LIMIT ?
        """, (chain['id'], hi - count + 1, hi, count))

        if hi is None:
            hi = int(rows[0][1])
        basename = os.path.basename(page['env']['PATH_INFO'])

        nav = ['<a href="',
               basename, '?count=', str(count), '">&lt;&lt;</a>']
        nav += [' <a href="', basename, '?hi=', str(hi + count),
                 '&amp;count=', str(count), '">&lt;</a>']
        nav += [' ', '&gt;']
        if hi >= count:
            nav[-1] = ['<a href="', basename, '?hi=', str(hi - count),
                        '&amp;count=', str(count), '">', nav[-1], '</a>']
        nav += [' ', '&gt;&gt;']
        if hi != count - 1:
            nav[-1] = ['<a href="', basename, '?hi=', str(count - 1),
                        '&amp;count=', str(count), '">', nav[-1], '</a>']
        for c in (20, 50, 100, 500, 1440):
            nav += [' ']
            if c != count:
                nav += ['<a href="', basename, '?count=', str(c)]
                if hi is not None:
                    nav += ['&amp;hi=', str(max(hi, c - 1))]
                nav += ['">']
            nav += [' ', str(c)]
            if c != count:
                nav += ['</a>']

        nav += [' <a href="', page['dotdot'], '">搜索</a>']

        extra = False
        #extra = True
        body += ['<p>', nav, '</p>\n',
                 '<table><tr><th>区块</th><th>大约生成时间</th>',
                 '<th>交易数</th><th>输出值</th>',
                 '<th>难度</th><th>已发行</th>',
                 '<th>平均币龄</th><th>币龄</th>',
                 '<th>% ',
                 '<a href="https://en.bitcoin.it/wiki/Bitcoin_Days_Destroyed">',
                 'CoinDD</a></th>',
                 ['<th>Satoshi-seconds</th>',
                  '<th>Total ss</th>']
                 if extra else '',
                 '</tr>\n']
        for row in rows:
            (hash, height, nTime, num_tx, nBits, value_out,
             seconds, ss, satoshis, destroyed, total_ss) = row
            nTime = int(nTime)
            value_out = int(value_out)
            seconds = int(seconds)
            satoshis = int(satoshis or 0)
            ss = int(ss or 0)
            total_ss = int(total_ss or 0)

            if satoshis == 0:
                avg_age = '&nbsp;'
            else:
                avg_age = '%5g' % (ss / satoshis / 86400.0)

            if seconds <= 0:
                percent_destroyed = '&nbsp;'
            else:
                try:
                    percent_destroyed = '%5g' % (
                        100.0 - (100.0 * ss / total_ss)) + '%'
                except:
                    percent_destroyed = '0%'

            body += [
                '<tr><td><a href="', page['dotdot'], 'block/',
                abe.store.hashout_hex(hash),
                '">', height, '</a>'
                '</td><td>', format_time(int(nTime)),
                '</td><td>', num_tx,
                '</td><td>', format_satoshis(value_out, chain),
                '</td><td>', util.calculate_difficulty(int(nBits)),
                '</td><td>', format_satoshis(satoshis, chain),
                '</td><td>', avg_age,
                '</td><td>', '%5g' % (seconds / 86400.0),
                '</td><td>', percent_destroyed,
                ['</td><td>', '%8g' % ss,
                 '</td><td>', '%8g' % total_ss] if extra else '',
                '</td></tr>\n']

        body += ['</table>\n<p>', nav, '</p>\n']

    def _show_block(abe, where, bind, page, dotdotblock, chain):
        address_version = ('\0' if chain is None
                           else chain['address_version'])
        body = page['body']
        sql = """
            SELECT
                block_id,
                block_hash,
                block_version,
                block_hashMerkleRoot,
                (block_nTime+28800),
                block_nBits,
                block_nNonce,
                block_height,
                prev_block_hash,
                block_chain_work,
                block_value_in,
                block_value_out,
                block_total_satoshis,
                block_total_seconds,
                block_satoshi_seconds,
                block_total_ss,
                block_ss_destroyed,
                block_num_tx
              FROM chain_summary
             WHERE """ + where
        row = abe.store.selectrow(sql, bind)
        if (row is None):
            body += ['<p class="error">Block not found.</p>']
            return
        (block_id, block_hash, block_version, hashMerkleRoot,
         nTime, nBits, nNonce, height,
         prev_block_hash, block_chain_work, value_in, value_out,
         satoshis, seconds, ss, total_ss, destroyed, num_tx) = (
            row[0], abe.store.hashout_hex(row[1]), row[2],
            abe.store.hashout_hex(row[3]), row[4], int(row[5]), row[6],
            row[7], abe.store.hashout_hex(row[8]),
            abe.store.binout_int(row[9]), int(row[10]), int(row[11]),
            None if row[12] is None else int(row[12]),
            None if row[13] is None else int(row[13]),
            None if row[14] is None else int(row[14]),
            None if row[15] is None else int(row[15]),
            None if row[16] is None else int(row[16]),
            int(row[17]),
            )

        next_list = abe.store.selectall("""
            SELECT DISTINCT n.block_hash, cc.in_longest
              FROM block_next bn
              JOIN block n ON (bn.next_block_id = n.block_id)
              JOIN chain_candidate cc ON (n.block_id = cc.block_id)
             WHERE bn.block_id = ?
             ORDER BY cc.in_longest DESC""",
                                  (block_id,))

        if chain is None:
            page['title'] = ['Block ', block_hash[:4], '...', block_hash[-10:]]
        else:
            page['title'] = [escape(chain['name']), ' ', height]
            page['h1'] = ['<a href="', page['dotdot'], 'chain/',
                          escape(chain['name']), '?hi=', height, '">',
                          escape(chain['name']), '</a> ', height]

        #body += abe.short_link(page, 'b/' + block_shortlink(block_hash))

        body += ['<p>Hash: ', block_hash, '<br />\n']

        if prev_block_hash is not None:
            body += ['前一个区块: <a href="', dotdotblock,
                     prev_block_hash, '">', prev_block_hash, '</a><br />\n']
        if next_list:
            body += ['后一个区块: ']
        for row in next_list:
            hash = abe.store.hashout_hex(row[0])
            body += ['<a href="', dotdotblock, hash, '">', hash, '</a><br />\n']

        body += [
            '高度: ', height, '<br />\n',
            '版本: ', block_version, '<br />\n',
            '交易 Merkle Root: ', hashMerkleRoot, '<br />\n',
            '时间: ', nTime, ' (', format_time(nTime), ')<br />\n',
            '难度: ', format_difficulty(util.calculate_difficulty(nBits)),
            ' (Bits: %x)' % (nBits,), '<br />\n',
            '累计难度: ', format_difficulty(
                util.work_to_difficulty(block_chain_work)), '<br />\n',
            'Nonce: ', nNonce, '<br />\n',
            '交易: ', num_tx, '<br />\n',
            '输出值: ', format_satoshis(value_out, chain), '<br />\n',

            ['平均币龄: %6g' % (ss / 86400.0 / satoshis,),
             ' 天<br />\n']
            if satoshis and (ss is not None) else '',

            '' if destroyed is None else
            ['Coin-days Destroyed: ',
             format_satoshis(destroyed / 86400.0, chain), '<br />\n'],

            ['累计 Coin-days Destroyed: %6g%%<br />\n' %
             (100 * (1 - float(ss or 0) / total_ss),)]
            if total_ss else '',

            ['sat=',satoshis,';sec=',seconds,';ss=',ss,
             ';total_ss=',total_ss,';destroyed=',destroyed]
            if abe.debug else '',

            '</p>\n']

        body += ['<h3>交易</h3>\n']

        tx_ids = []
        txs = {}
        block_out = 0
        block_in = 0
        for row in abe.store.selectall("""
            SELECT tx_id, tx_hash, tx_size, txout_value, pubkey_hash
              FROM txout_detail
             WHERE block_id = ?
             ORDER BY tx_pos, txout_pos
        """, (block_id,)):
            tx_id, tx_hash_hex, tx_size, txout_value, pubkey_hash = (
                row[0], abe.store.hashout_hex(row[1]), int(row[2]),
                int(row[3]), abe.store.binout(row[4]))
            tx = txs.get(tx_id)
            if tx is None:
                tx_ids.append(tx_id)
                txs[tx_id] = {
                    "hash": tx_hash_hex,
                    "total_out": 0,
                    "total_in": 0,
                    "out": [],
                    "in": [],
                    "size": tx_size,
                    }
                tx = txs[tx_id]
            tx['total_out'] += txout_value
            block_out += txout_value
            tx['out'].append({
                    "value": txout_value,
                    "pubkey_hash": pubkey_hash,
                    })
        for row in abe.store.selectall("""
            SELECT tx_id, txin_value, pubkey_hash
              FROM txin_detail
             WHERE block_id = ?
             ORDER BY tx_pos, txin_pos
        """, (block_id,)):
            tx_id, txin_value, pubkey_hash = (
                row[0], 0 if row[1] is None else int(row[1]),
                abe.store.binout(row[2]))
            tx = txs.get(tx_id)
            if tx is None:
                # Strange, inputs but no outputs?
                tx_ids.append(tx_id)
                #row2 = abe.store.selectrow("""
                #    SELECT tx_hash, tx_size FROM tx WHERE tx_id = ?""",
                #                           (tx_id,))
                txs[tx_id] = {
                    "hash": "AssertionFailedTxInputNoOutput",
                    "total_out": 0,
                    "total_in": 0,
                    "out": [],
                    "in": [],
                    "size": -1,
                    }
                tx = txs[tx_id]
            tx['total_in'] += txin_value
            block_in += txin_value
            tx['in'].append({
                    "value": txin_value,
                    "pubkey_hash": pubkey_hash,
                    })

        body += ['<table><tr><th>交易</th><th>税</th>'
                 '<th>大小 (kB)</th><th>从 (数量)</th><th>到 (数量)</th>'
                 '</tr>\n']

        txnum = 0
        posgen = 0

        for tx_id in tx_ids:
            txnum += 1
            tx = txs[tx_id]
            is_coinbase = (tx_id == tx_ids[0])
            fees = tx['total_in'] - tx['total_out']
            if is_coinbase:
                fees = 0
            if txnum == 2 and block_hash[:4] > '0000':
                posgen = abs(fees)
                fees = 0


            body += ['<tr><td><a href="../tx/' + tx['hash'] + '">',
                     tx['hash'][:10], '...</a>'
                     '</td><td>', format_satoshis(fees, chain),
                     '</td><td>', tx['size'] / 1000.0,
                     '</td><td>']
            if is_coinbase:
               pgen = 0
               gen = tx['total_out'] - tx['total_in']

               if block_hash[:4] > '0000':
                  body += [' Total']
               else:
                   gen = format_satoshis(gen, chain)
                   fees = format_satoshis(fees, chain)
                   body += ['Generation: ', gen , ' Total']
                   page['h1'] = ['<a href="', page['dotdot'], 'chain/',
                                 escape('Ybcoin'), '?hi=', height, '">',
                                 escape('Ybcoin'), '</a> ', height,'<br />','<FONT SIZE="-1">Proof of Work; 生成',
                                 gen,' 个币 </FONT>']
            else:
                for txin in tx['in']:
                    body += hash_to_address_link(
                        address_version, txin['pubkey_hash'], page['dotdot'])
                    body += [': ', format_satoshis(txin['value'], chain),
                             '<br />']
            body += ['</td><td>']
            for txout in tx['out']:

              if txout['value'] > 0:
                  body += hash_to_address_link(
                  address_version, txout['pubkey_hash'], page['dotdot'])
                  body += [': ', format_satoshis(txout['value'], chain), '<br />']
              else:
                   if txnum ==1:
                        body += 'Generated coins are sent in the next transaction'
            body += ['</td></tr>\n']
        if block_hash[:4] > '0000':
               posgen = format_satoshis(posgen,chain)
               txt ='POS Generation: ' + posgen
               pos = body.index(' Total')
               body.insert(pos,txt)
               page['h1'] = ['<a href="', page['dotdot'], 'chain/',
                             escape('Ybcoin'), '?hi=', height, '">',
                             escape('Ybcoin'), '</a> ', height,'<br />','<FONT COLOR="FF0000"><FONT SIZE="-1">Proof of Stake; </FONT></FONT>',
                             '<FONT SIZE="-1">生成 ',posgen, ' 个币</FONT>','\n']

        body += '</table>\n'

    def handle_block(abe, page):
        block_hash = wsgiref.util.shift_path_info(page['env'])
        if block_hash in (None, '') or page['env']['PATH_INFO'] != '':
            raise PageNotFound()

        block_hash = block_hash.lower()  # Case-insensitive, BBE compatible
        page['title'] = '区块'

        if not is_hash_prefix(block_hash):
            page['body'] += ['<p class="error">Not a valid block hash.</p>']
            return

        # Try to show it as a block number, not a block hash.

        dbhash = abe.store.hashin_hex(block_hash)

        # XXX arbitrary choice: minimum chain_id.  Should support
        # /chain/CHAIN/block/HASH URLs and try to keep "next block"
        # links on the chain.
        row = abe.store.selectrow("""
            SELECT MIN(cc.chain_id), cc.block_id, cc.block_height
              FROM chain_candidate cc
              JOIN block b ON (cc.block_id = b.block_id)
             WHERE b.block_hash = ? AND cc.in_longest = 1
             GROUP BY cc.block_id, cc.block_height""",
            (dbhash,))
        if row is None:
            abe._show_block('block_hash = ?', (dbhash,), page, '', None)
        else:
            chain_id, block_id, height = row
            chain = abe.chain_lookup_by_id(chain_id)
            page['title'] = [escape(chain['name']), ' ', height]
            abe._show_block('block_id = ?', (block_id,), page, '', chain)

    def handle_tx(abe, page):
        tx_hash = wsgiref.util.shift_path_info(page['env'])
        if tx_hash in (None, '') or page['env']['PATH_INFO'] != '':
            raise PageNotFound()

        tx_hash = tx_hash.lower()  # Case-insensitive, BBE compatible
        page['title'] = ['交易 ', tx_hash[:10], '...', tx_hash[-4:]]
        body = page['body']

        if not is_hash_prefix(tx_hash):
            body += ['<p class="error">Not a valid transaction hash.</p>']
            return

        row = abe.store.selectrow("""
            SELECT tx_id, tx_version, tx_lockTime, tx_size
              FROM tx
             WHERE tx_hash = ?
        """, (abe.store.hashin_hex(tx_hash),))
        if row is None:
            body += ['<p class="error">Transaction not found.</p>']
            return
        tx_id, tx_version, tx_lockTime, tx_size = (
            int(row[0]), int(row[1]), int(row[2]), int(row[3]))

        block_rows = abe.store.selectall("""
            SELECT c.chain_name, cc.in_longest,
                   (b.block_nTime+28800), b.block_height, b.block_hash,
                   block_tx.tx_pos
              FROM chain c
              JOIN chain_candidate cc ON (cc.chain_id = c.chain_id)
              JOIN block b ON (b.block_id = cc.block_id)
              JOIN block_tx ON (block_tx.block_id = b.block_id)
             WHERE block_tx.tx_id = ?
             ORDER BY c.chain_id, cc.in_longest DESC, b.block_hash
        """, (tx_id,))

        def parse_row(row):
            pos, script, value, o_hash, o_pos, binaddr = row
            return {
                "pos": int(pos),
                "script": abe.store.binout(script),
                "value": None if value is None else int(value),
                "o_hash": abe.store.hashout_hex(o_hash),
                "o_pos": None if o_pos is None else int(o_pos),
                "binaddr": abe.store.binout(binaddr),
                }

        def row_to_html(row, this_ch, other_ch, no_link_text):
            body = page['body']
            body += [
                '<tr>\n',
                '<td><a name="', this_ch, row['pos'], '">', row['pos'],
                '</a></td>\n<td>']
            if row['o_hash'] is None:
                body += [no_link_text]
            else:
                body += [
                    '<a href="', row['o_hash'], '#', other_ch, row['o_pos'],
                    '">', row['o_hash'][:10], '...:', row['o_pos'], '</a>']
            body += [
                '</td>\n',
                '<td>', format_satoshis(row['value'], chain), '</td>\n',
                '<td>']
            if row['binaddr'] is None:
                body += ['Unknown']
            else:
                body += hash_to_address_link(chain['address_version'],
                                             row['binaddr'], '../')
            body += ['</td>\n']
            if row['script'] is not None:
                body += ['<td>', escape(decode_script(row['script'])),
                '</td>\n']
            body += ['</tr>\n']

        # XXX Unneeded outer join.
        in_rows = map(parse_row, abe.store.selectall("""
            SELECT
                txin.txin_pos""" + (""",
                txin.txin_scriptSig""" if abe.store.keep_scriptsig else """,
                NULL""") + """,
                txout.txout_value,
                COALESCE(prevtx.tx_hash, u.txout_tx_hash),
                COALESCE(txout.txout_pos, u.txout_pos),
                pubkey.pubkey_hash
              FROM txin
              LEFT JOIN txout ON (txout.txout_id = txin.txout_id)
              LEFT JOIN pubkey ON (pubkey.pubkey_id = txout.pubkey_id)
              LEFT JOIN tx prevtx ON (txout.tx_id = prevtx.tx_id)
              LEFT JOIN unlinked_txin u ON (u.txin_id = txin.txin_id)
             WHERE txin.tx_id = ?
             ORDER BY txin.txin_pos
        """, (tx_id,)))

        # XXX Only two outer JOINs needed.
        out_rows = map(parse_row, abe.store.selectall("""
            SELECT
                txout.txout_pos,
                txout.txout_scriptPubKey,
                txout.txout_value,
                nexttx.tx_hash,
                txin.txin_pos,
                pubkey.pubkey_hash
              FROM txout
              LEFT JOIN txin ON (txin.txout_id = txout.txout_id)
              LEFT JOIN pubkey ON (pubkey.pubkey_id = txout.pubkey_id)
              LEFT JOIN tx nexttx ON (txin.tx_id = nexttx.tx_id)
             WHERE txout.tx_id = ?
             ORDER BY txout.txout_pos
        """, (tx_id,)))

        def sum_values(rows):
            ret = 0
            for row in rows:
                if row['value'] is None:
                    return None
                ret += row['value']
            return ret

        value_in = sum_values(in_rows)
        value_out = sum_values(out_rows)
        is_coinbase = None

        body += abe.short_link(page, 't/' + hexb58(tx_hash[:14]))
        body += ['<p>Hash: ', tx_hash, '<br />\n']
        chain = None
        for row in block_rows:
            (name, in_longest, nTime, height, blk_hash, tx_pos) = (
                row[0], int(row[1]), int(row[2]), int(row[3]),
                abe.store.hashout_hex(row[4]), int(row[5]))
            if chain is None:
                chain = abe.chain_lookup_by_name(name)
                is_coinbase = (tx_pos == 0)
            elif name <> chain['name']:
                abe.log.warn('Transaction ' + tx_hash + ' in multiple chains: '
                             + name + ', ' + chain['name'])
            body += [
                '出现在 <a href="../block/', blk_hash, '">',
                escape(name), ' ',
                height if in_longest else [blk_hash[:10], '...', blk_hash[-4:]],
                '</a> (', format_time(nTime), ')<br />\n']

        if chain is None:
            abe.log.warn('Assuming default chain for Transaction ' + tx_hash)
            chain = abe.get_default_chain()

        body += [
            '输入数量: ', len(in_rows),
            ' (<a href="#inputs">跳转到输入</a>)<br />\n',
            '输入总量: ', format_satoshis(value_in, chain), '<br />\n',
            '输出数量: ', len(out_rows),
            ' (<a href="#outputs">跳转到输出</a>)<br />\n',
            '输出总量: ', format_satoshis(value_out, chain), '<br />\n',
            '大小: ', tx_size, ' bytes<br />\n',
            '税费: ', format_satoshis(0 if is_coinbase else
                                     (value_in and value_out and
                                      value_in - value_out), chain),
            '<br />\n',
            '<a href="../rawtx/', tx_hash, '">交易原始数据</a><br />\n']
        body += ['</p>\n',
                 '<a name="inputs"><h3>输入</h3></a>\n<table>\n',
                 '<tr><th>序号</th><th>前一个输出</th><th>数量</th>',
                 '<th>从</th>']
        if abe.store.keep_scriptsig:
            body += ['<th>ScriptSig</th>']
        body += ['</tr>\n']
        for row in in_rows:
            row_to_html(row, 'i', 'o',
                        'Generation' if is_coinbase else 'Unknown')
        body += ['</table>\n',
                 '<a name="outputs"><h3>输出</h3></a>\n<table>\n',
                 '<tr><th>序号</th><th>消费的输入</th><th>数量</th>',
                 '<th>到</th><th>ScriptPubKey</th></tr>\n']
        for row in out_rows:
            row_to_html(row, 'o', 'i', 'Not yet redeemed')

        body += ['</table>\n']

    def handle_rawtx(abe, page):
        abe.do_raw(page, abe.do_rawtx(page))

    def do_rawtx(abe, page):
        tx_hash = wsgiref.util.shift_path_info(page['env'])
        if tx_hash in (None, '') or page['env']['PATH_INFO'] != '' \
                or not is_hash_prefix(tx_hash):
            return 'ERROR: Not in correct format'  # BBE compatible

        tx = abe.store.export_tx(tx_hash=tx_hash.lower())
        if tx is None:
            return 'ERROR: Transaction does not exist.'  # BBE compatible
        return json.dumps(tx, sort_keys=True, indent=2)

    def handle_address(abe, page):
        address = wsgiref.util.shift_path_info(page['env'])
        if address in (None, '') or page['env']['PATH_INFO'] != '':
            raise PageNotFound()

        body = page['body']
        page['title'] = 'Address ' + escape(address)
        version, binaddr = util.decode_check_address(address)
        if binaddr is None:
            body += ['<p>Not a valid address.</p>']
            return

        dbhash = abe.store.binin(binaddr)

        chains = {}
        balance = {}
        received = {}
        sent = {}
        count = [0, 0]
        chain_ids = []
        def adj_balance(txpoint):
            chain_id = txpoint['chain_id']
            value = txpoint['value']
            if chain_id not in balance:
                chain_ids.append(chain_id)
                chains[chain_id] = abe.chain_lookup_by_id(chain_id)
                balance[chain_id] = 0
                received[chain_id] = 0
                sent[chain_id] = 0
            balance[chain_id] += value
            if value > 0:
                received[chain_id] += value
            else:
                sent[chain_id] -= value
            count[txpoint['is_in']] += 1

        txpoints = []
        max_rows = abe.address_history_rows_max
        in_rows = abe.store.selectall("""
            SELECT
                (b.block_nTime+28800),
                cc.chain_id,
                b.block_height,
                1,
                b.block_hash,
                tx.tx_hash,
                txin.txin_pos,
                -prevout.txout_value
              FROM chain_candidate cc
              JOIN block b ON (b.block_id = cc.block_id)
              JOIN block_tx ON (block_tx.block_id = b.block_id)
              JOIN tx ON (tx.tx_id = block_tx.tx_id)
              JOIN txin ON (txin.tx_id = tx.tx_id)
              JOIN txout prevout ON (txin.txout_id = prevout.txout_id)
              JOIN pubkey ON (pubkey.pubkey_id = prevout.pubkey_id)
             WHERE pubkey.pubkey_hash = ?
               AND cc.in_longest = 1""" + ("" if max_rows < 0 else """
             LIMIT ?"""),
                      (dbhash,)
                      if max_rows < 0 else
                      (dbhash, max_rows + 1))

        too_many = False
        if max_rows >= 0 and len(in_rows) > max_rows:
            too_many = True

        if not too_many:
            out_rows = abe.store.selectall("""
                SELECT
                    (b.block_nTime+28800),
                    cc.chain_id,
                    b.block_height,
                    0,
                    b.block_hash,
                    tx.tx_hash,
                    txout.txout_pos,
                    txout.txout_value
                  FROM chain_candidate cc
                  JOIN block b ON (b.block_id = cc.block_id)
                  JOIN block_tx ON (block_tx.block_id = b.block_id)
                  JOIN tx ON (tx.tx_id = block_tx.tx_id)
                  JOIN txout ON (txout.tx_id = tx.tx_id)
                  JOIN pubkey ON (pubkey.pubkey_id = txout.pubkey_id)
                 WHERE pubkey.pubkey_hash = ?
                   AND cc.in_longest = 1""" + ("" if max_rows < 0 else """
                 LIMIT ?"""),
                          (dbhash, max_rows + 1)
                          if max_rows >= 0 else
                          (dbhash,))
            if max_rows >= 0 and len(out_rows) > max_rows:
                too_many = True

        if too_many:
            body += ["<p>I'm sorry, this address has too many records"
                     " to display.</p>"]
            return

        rows = []
        rows += in_rows
        rows += out_rows
        rows.sort()
        for row in rows:
            nTime, chain_id, height, is_in, blk_hash, tx_hash, pos, value = row
            txpoint = {
                    "nTime":    int(nTime),
                    "chain_id": int(chain_id),
                    "height":   int(height),
                    "is_in":    int(is_in),
                    "blk_hash": abe.store.hashout_hex(blk_hash),
                    "tx_hash":  abe.store.hashout_hex(tx_hash),
                    "pos":      int(pos),
                    "value":    int(value),
                    }
            adj_balance(txpoint)
            txpoints.append(txpoint)

        if (not chain_ids):
            body += ['<p>Address not seen on the network.</p>']
            return

        def format_amounts(amounts, link):
            ret = []
            for chain_id in chain_ids:
                chain = chains[chain_id]
                if chain_id != chain_ids[0]:
                    ret += [', ']
                ret += [format_satoshis(amounts[chain_id], chain),
                        ' ', escape(chain['code3'])]
                if link:
                    other = util.hash_to_address(
                        chain['address_version'], binaddr)
                    if other != address:
                        ret[-1] = ['<a href="', page['dotdot'],
                                   'address/', other,
                                   '">', ret[-1], '</a>']
            return ret

        if abe.shortlink_type == "firstbits":
            link = abe.store.get_firstbits(
                address_version=version, db_pubkey_hash=dbhash,
                chain_id = (page['chain'] and page['chain']['id']))
            if link:
                link = link.replace('l', 'L')
            else:
                link = address
        else:
            link = address[0 : abe.shortlink_type]
        body += abe.short_link(page, 'a/' + link)

        body += ['<p>余额: '] + format_amounts(balance, True)

        for chain_id in chain_ids:
            balance[chain_id] = 0  # Reset for history traversal.

        body += ['<br />\n',
                 '交易: ', count[0], '<br />\n',
                 '收到: ', format_amounts(received, False), '<br />\n',
                 '交易输出: ', count[1], '<br />\n',
                 '发出: ', format_amounts(sent, False), '<br />\n']

        body += ['</p>\n'
                 '<h3>交易</h3>\n'
                 '<table>\n<tr><th>交易</th><th>区块</th>'
                 '<th>大约生成时间</th><th>数量</th><th>余额</th>'
                 '<th>货币</th></tr>\n']

        for elt in txpoints:
            chain = chains[elt['chain_id']]
            balance[elt['chain_id']] += elt['value']
            body += ['<tr><td><a href="../tx/', elt['tx_hash'],
                     '#', 'i' if elt['is_in'] else 'o', elt['pos'],
                     '">', elt['tx_hash'][:10], '...</a>',
                     '</td><td><a href="../block/', elt['blk_hash'],
                     '">', elt['height'], '</a></td><td>',
                     format_time(elt['nTime']), '</td><td>']
            if elt['value'] < 0:
                body += ['(', format_satoshis(-elt['value'], chain), ')']
            else:
                body += [format_satoshis(elt['value'], chain)]
            body += ['</td><td>',
                     format_satoshis(balance[elt['chain_id']], chain),
                     '</td><td>', escape(chain['code3']),
                     '</td></tr>\n']
        body += ['</table>\n']

    def search_form(abe, page):
        q = (page['params'].get('q') or [''])[0]
        return [
            '<p>输入地址、区块序号或hash、交易ID或公钥hash、币种进行搜索:</p>\n'
            '<form action="', page['dotdot'], 'search"><p>\n'
            '<input name="q" size="64" value="', escape(q), '" />'
            '<button type="submit">搜索</button>\n'
            '<br />地址或者hash值至少包含前 ',
            HASH_PREFIX_MIN, ' 个字符</p></form>\n']

    def handle_search(abe, page):
        page['title'] = 'Search'
        q = (page['params'].get('q') or [''])[0]
        if q == '':
            page['body'] = [
                '<p>请输入查询内容</p>\n', abe.search_form(page)]
            return

        found = []
        if HEIGHT_RE.match(q):      found += abe.search_number(int(q))
        if util.possible_address(q):found += abe.search_address(q)
        elif ADDR_PREFIX_RE.match(q):found += abe.search_address_prefix(q)
        if is_hash_prefix(q):       found += abe.search_hash_prefix(q)
        found += abe.search_general(q)
        abe.show_search_results(page, found)

    def show_search_results(abe, page, found):
        if not found:
            page['body'] = [
                '<p>No results found.</p>\n', abe.search_form(page)]
            return

        if len(found) == 1:
            # Undo shift_path_info.
            sn = posixpath.dirname(page['env']['SCRIPT_NAME'])
            if sn == '/': sn = ''
            page['env']['SCRIPT_NAME'] = sn
            page['env']['PATH_INFO'] = '/' + page['dotdot'] + found[0]['uri']
            del(page['env']['QUERY_STRING'])
            raise Redirect()

        body = page['body']
        body += ['<h3>Search Results</h3>\n<ul>\n']
        for result in found:
            body += [
                '<li><a href="', page['dotdot'], escape(result['uri']), '">',
                escape(result['name']), '</a></li>\n']
        body += ['</ul>\n']

    def search_number(abe, n):
        def process(row):
            (chain_name, dbhash, in_longest) = row
            hexhash = abe.store.hashout_hex(dbhash)
            if in_longest == 1:
                name = str(n)
            else:
                name = hexhash
            return {
                'name': chain_name + ' ' + name,
                'uri': 'block/' + hexhash,
                }

        return map(process, abe.store.selectall("""
            SELECT c.chain_name, b.block_hash, cc.in_longest
              FROM chain c
              JOIN chain_candidate cc ON (cc.chain_id = c.chain_id)
              JOIN block b ON (b.block_id = cc.block_id)
             WHERE cc.block_height = ?
             ORDER BY c.chain_name, cc.in_longest DESC
        """, (n,)))

    def search_hash_prefix(abe, q, types = ('tx', 'block', 'pubkey')):
        q = q.lower()
        ret = []
        for t in types:
            def process(row):
                if   t == 'tx':    name = 'Transaction'
                elif t == 'block': name = 'Block'
                else:
                    # XXX Use Bitcoin address version until we implement
                    # /pubkey/... for this to link to.
                    return abe._found_address(
                        util.hash_to_address('\0', abe.store.binout(row[0])))
                hash = abe.store.hashout_hex(row[0])
                return {
                    'name': name + ' ' + hash,
                    'uri': t + '/' + hash,
                    }

            if t == 'pubkey':
                if len(q) > 40:
                    continue
                lo = abe.store.binin_hex(q + '0' * (40 - len(q)))
                hi = abe.store.binin_hex(q + 'f' * (40 - len(q)))
            else:
                lo = abe.store.hashin_hex(q + '0' * (64 - len(q)))
                hi = abe.store.hashin_hex(q + 'f' * (64 - len(q)))

            ret += map(process, abe.store.selectall(
                "SELECT " + t + "_hash FROM " + t + " WHERE " + t +
                # XXX hardcoded limit.
                "_hash BETWEEN ? AND ? LIMIT 100",
                (lo, hi)))
        return ret

    def _found_address(abe, address):
        return { 'name': 'Address ' + address, 'uri': 'address/' + address }

    def search_address(abe, address):
        try:
            binaddr = base58.bc_address_to_hash_160(address)
        except:
            return abe.search_address_prefix(address)
        return [abe._found_address(address)]

    def search_address_prefix(abe, ap):
        ret = []
        ones = 0
        for c in ap:
            if c != '1':
                break
            ones += 1
        all_ones = (ones == len(ap))
        minlen = max(len(ap), 24)
        l = max(35, len(ap))  # XXX Increase "35" to support multibyte
                              # address versions.
        al = ap + ('1' * (l - len(ap)))
        ah = ap + ('z' * (l - len(ap)))

        def incr_str(s):
            for i in range(len(s)-1, -1, -1):
                if s[i] != '\xff':
                    return s[:i] + chr(ord(s[i])+1) + ('\0' * (len(s) - i - 1))
            return '\1' + ('\0' * len(s))

        def process(row):
            hash = abe.store.binout(row[0])
            address = util.hash_to_address(vl, hash)
            if address.startswith(ap):
                v = vl
            else:
                if vh != vl:
                    address = util.hash_to_address(vh, hash)
                    if not address.startswith(ap):
                        return None
                    v = vh
            if abe.is_address_version(v):
                return abe._found_address(address)

        while l >= minlen:
            vl, hl = util.decode_address(al)
            vh, hh = util.decode_address(ah)
            if ones:
                if not all_ones and \
                        util.hash_to_address('\0', hh)[ones:][:1] == '1':
                    break
            elif vh == '\0':
                break
            elif vh != vl and vh != incr_str(vl):
                continue
            if hl <= hh:
                neg = ""
            else:
                neg = " NOT"
                hl, hh = hh, hl
            bl = abe.store.binin(hl)
            bh = abe.store.binin(hh)
            ret += filter(None, map(process, abe.store.selectall(
                "SELECT pubkey_hash FROM pubkey WHERE pubkey_hash" +
                # XXX hardcoded limit.
                neg + " BETWEEN ? AND ? LIMIT 100", (bl, bh))))
            l -= 1
            al = al[:-1]
            ah = ah[:-1]

        return ret

    def search_general(abe, q):
        """搜索非地址、hash或区块id的内容.
        现在仅限于 chain 名称和货币代号"""
        def process(row):
            (name, code3) = row
            return { 'name': name + ' (' + code3 + ')',
                     'uri': 'chain/' + str(name) }
        ret = map(process, abe.store.selectall("""
            SELECT chain_name, chain_code3
              FROM chain
             WHERE UPPER(chain_name) LIKE '%' || ? || '%'
                OR UPPER(chain_code3) LIKE '%' || ? || '%'
        """, (q.upper(), q.upper())))
        return ret

    def handle_t(abe, page):
        abe.show_search_results(
            page,
            abe.search_hash_prefix(
                b58hex(wsgiref.util.shift_path_info(page['env'])),
                ('tx',)))

    def handle_b(abe, page):
        if 'chain' in page:
            chain = page['chain']
            height = wsgiref.util.shift_path_info(page['env'])
            try:
                height = int(height)
            except:
                raise PageNotFound()
            if height < 0 or page['env']['PATH_INFO'] != '':
                raise PageNotFound()

            cmd = wsgiref.util.shift_path_info(page['env'])
            if cmd is not None:
                raise PageNotFound()  # XXX want to support /a/...

            page['title'] = [escape(chain['name']), ' ', height]
            abe._show_block(
                'chain_id = ? AND block_height = ? AND in_longest = 1',
                (chain['id'], height), page, page['dotdot'] + 'block/', chain)
            return

        abe.show_search_results(
            page,
            abe.search_hash_prefix(
                shortlink_block(wsgiref.util.shift_path_info(page['env'])),
                ('block',)))

    def handle_a(abe, page):
        arg = wsgiref.util.shift_path_info(page['env'])
        if abe.shortlink_type == "firstbits":
            addrs = map(
                abe._found_address,
                abe.store.firstbits_to_addresses(
                    arg.lower(),
                    chain_id = page['chain'] and page['chain']['id']))
        else:
            addrs = abe.search_address_prefix(arg)
        abe.show_search_results(page, addrs)

    def handle_unspent(abe, page):
        abe.do_raw(page, abe.do_unspent(page))

    def do_unspent(abe, page):
        addrs = wsgiref.util.shift_path_info(page['env'])
        if addrs is None:
            addrs = []
        else:
            addrs = addrs.split("|");
        if len(addrs) < 1 or len(addrs) > MAX_UNSPENT_ADDRESSES:
            return 'Number of addresses must be between 1 and ' + \
                str(MAX_UNSPENT_ADDRESSES)

        # XXX support multiple implementations while testing.
        impl = page['params'].get('impl', ['2'])[0]

        if page['chain']:
            chain_id = page['chain']['id']
            bind = [chain_id]
        else:
            chain_id = None
            bind = []

        hashes = []
        good_addrs = []
        for address in addrs:
            try:
                hashes.append(abe.store.binin(
                        base58.bc_address_to_hash_160(address)))
                good_addrs.append(address)
            except:
                pass
        addrs = good_addrs
        bind += hashes

        if len(hashes) == 0:  # Address(es) are invalid.
            return 'Error getting unspent outputs'  # blockchain.info compatible

        placeholders = "?" + (",?" * (len(hashes)-1))

        max_rows = abe.address_history_rows_max
        if max_rows >= 0:
            bind += [max_rows + 1]

        if impl == '1':
            rows = abe.store.selectall("""
                SELECT tod.tx_hash,
                       tod.txout_pos,
                       tod.txout_scriptPubKey,
                       tod.txout_value,
                       tod.block_height
                  FROM txout_detail tod
                  LEFT JOIN txin_detail tid ON (
                             tid.chain_id = tod.chain_id
                         AND tid.prevout_id = tod.txout_id
                         AND tid.in_longest = 1)
                 WHERE tod.in_longest = 1
                   AND tid.prevout_id IS NULL""" + ("" if chain_id is None else """
                   AND tod.chain_id = ?""") + """
                   AND tod.pubkey_hash IN (""" + placeholders + """)
                 ORDER BY tod.block_height,
                       tod.tx_pos,
                       tod.txout_pos""" + ("" if max_rows < 0 else """
                 LIMIT ?"""),
                                       tuple(bind))

            if max_rows >= 0 and len(rows) > max_rows:
                return "ERROR: too many records to display"

        else:
            spent = set()
            for txout_id, spent_chain_id in abe.store.selectall("""
                SELECT txin.txout_id, cc.chain_id
                  FROM chain_candidate cc
                  JOIN block_tx ON (block_tx.block_id = cc.block_id)
                  JOIN txin ON (txin.tx_id = block_tx.tx_id)
                  JOIN txout prevout ON (txin.txout_id = prevout.txout_id)
                  JOIN pubkey ON (pubkey.pubkey_id = prevout.pubkey_id)
                 WHERE cc.in_longest = 1""" + ("" if chain_id is None else """
                   AND cc.chain_id = ?""") + """
                   AND pubkey.pubkey_hash IN (""" + placeholders + """)""" + (
                    "" if max_rows < 0 else """
                 LIMIT ?"""), bind):
                spent.add((int(txout_id), int(spent_chain_id)))

            abe.log.debug('spent: %s', spent)

            received_rows = abe.store.selectall("""
                SELECT
                    txout.txout_id,
                    cc.chain_id,
                    tx.tx_hash,
                    txout.txout_pos,
                    txout.txout_scriptPubKey,
                    txout.txout_value,
                    cc.block_height
                  FROM chain_candidate cc
                  JOIN block_tx ON (block_tx.block_id = cc.block_id)
                  JOIN tx ON (tx.tx_id = block_tx.tx_id)
                  JOIN txout ON (txout.tx_id = tx.tx_id)
                  JOIN pubkey ON (pubkey.pubkey_id = txout.pubkey_id)
                 WHERE cc.in_longest = 1""" + ("" if chain_id is None else """
                   AND cc.chain_id = ?""") + """
                   AND pubkey.pubkey_hash IN (""" + placeholders + """)""" + (
                    "" if max_rows < 0 else """
                 ORDER BY cc.block_height,
                       block_tx.tx_pos,
                       txout.txout_pos
                 LIMIT ?"""), bind)
                
            if max_rows >= 0 and len(received_rows) > max_rows:
                return "ERROR: too many records to process"

            rows = []
            for row in received_rows:
                key = (int(row[0]), int(row[1]))
                if key in spent:
                    continue
                rows.append(row[2:])

        if len(rows) == 0:
            return 'No free outputs to spend [' + '|'.join(addrs) + ']'

        out = []
        for row in rows:
            tx_hash, out_pos, script, value, height = row
            tx_hash = abe.store.hashout_hex(tx_hash)
            out_pos = None if out_pos is None else int(out_pos)
            script = abe.store.binout_hex(script)
            value = None if value is None else int(value)
            height = None if height is None else int(height)
            out.append({
                    'tx_hash': tx_hash,
                    'tx_output_n': out_pos,
                    'script': script,
                    'value': value,
                    'value_hex': None if value is None else "%x" % value,
                    'block_number': height})

        return json.dumps({ 'unspent_outputs': out }, sort_keys=True, indent=2)

    def do_raw(abe, page, body):
        page['content_type'] = 'text/plain'
        page['template'] = '%(body)s'
        page['body'] = body

    def handle_q(abe, page):
        cmd = wsgiref.util.shift_path_info(page['env'])
        if cmd is None:
            return abe.q(page)

        func = getattr(abe, 'q_' + cmd, None)
        if func is None:
            raise PageNotFound()

        abe.do_raw(page, func(page, page['chain']))

    def q(abe, page):
        page['body'] = ['<p>支持的 APIs:</p>\n<ul>\n']
        for name in dir(abe):
            if not name.startswith("q_"):
                continue
            cmd = name[2:]
            page['body'] += ['<li><a href="q/', cmd, '">', cmd, '</a>']
            val = getattr(abe, name)
            if val.__doc__ is not None:
                page['body'] += [' - ', escape(val.__doc__)]
            page['body'] += ['</li>\n']
        page['body'] += ['</ul>\n']

    def get_max_block_height(abe, chain):
        # "getblockcount" traditionally returns max(block_height),
        # which is one less than the actual block count.
        return abe.store.get_block_number(chain['id'])

    def q_getblockcount(abe, page, chain):
        """显示当前区块数量"""
        if chain is None:
            return '显示 CHAIN 当前区块数量.\n' \
                '/chain/CHAIN/q/getblockcount\n'
        return abe.get_max_block_height(chain)

    def q_getdifficulty(abe, page, chain):
        """显示当前难度"""
        if chain is None:
            return '显示 CHAIN 最新区块难度.\n' \
                '/chain/CHAIN/q/getdifficulty\n'
        target = abe.store.get_target(chain['id'])
        return "" if target is None else util.target_to_difficulty(target)

    def q_translate_address(abe, page, chain):
        """将地址hash转换为地址."""
        addr = wsgiref.util.shift_path_info(page['env'])
        if chain is None or addr is None:
            return '转换 CHAIN 地址.\n' \
                '/chain/CHAIN/q/translate_address/ADDRESS\n'
        version, hash = util.decode_check_address(addr)
        if hash is None:
            return addr + " (INVALID ADDRESS)"
        return util.hash_to_address(chain['address_version'], hash)

    def q_decode_address(abe, page, chain):
        """显示地址版本的前缀和hash."""
        addr = wsgiref.util.shift_path_info(page['env'])
        if addr is None:
            return "以十六进制字符串形式显示地址版本字节和公钥，用(‘:’)隔开.\n" \
                '/q/decode_address/ADDRESS\n'
        # XXX error check?
        version, hash = util.decode_address(addr)
        ret = version.encode('hex') + ":" + hash.encode('hex')
        if util.hash_to_address(version, hash) != addr:
            ret = "INVALID(" + ret + ")"
        return ret

    def q_addresstohash(abe, page, chain):
        """显示地址的公钥hash."""
        addr = wsgiref.util.shift_path_info(page['env'])
        if addr is None:
            return '显示地址的160-bit hash，为了兼容BBE，地址没有做有效性检查.\n' \
                '参见 /q/decode_address.\n' \
                '/q/addresstohash/ADDRESS\n'
        version, hash = util.decode_address(addr)
        return hash.encode('hex').upper()

    def q_hashtoaddress(abe, page, chain):
        """将地址版本前缀和hash转换为地址."""
        arg1 = wsgiref.util.shift_path_info(page['env'])
        arg2 = wsgiref.util.shift_path_info(page['env'])
        if arg1 is None:
            return \
                '将 160-bit hash和地址版本前缀转换为地址.\n' \
                '/q/hashtoaddress/HASH[/VERSION]\n'

        if page['env']['PATH_INFO']:
            return "ERROR: Too many arguments"

        if arg2 is not None:
            # BBE-compatible HASH/VERSION
            version, hash = arg2, arg1

        elif arg1.find(":") >= 0:
            # VERSION:HASH as returned by /q/decode_address.
            version, hash = arg1.split(":", 1)

        elif chain:
            version, hash = chain['address_version'].encode('hex'), arg1

        else:
            # Default: Bitcoin address starting with "1".
            version, hash = '00', arg1

        try:
            hash = hash.decode('hex')
            version = version.decode('hex')
        except:
            return 'ERROR: Arguments must be hexadecimal strings of even length'
        return util.hash_to_address(version, hash)

    def q_hashpubkey(abe, page, chain):
        """显示给定公钥的160-bit的hash."""
        pubkey = wsgiref.util.shift_path_info(page['env'])
        if pubkey is None:
            return \
                "返回 PUBKEY 的160-bit的hash.\n" \
                "/q/hashpubkey/PUBKEY\n"
        try:
            pubkey = pubkey.decode('hex')
        except:
            return 'ERROR: invalid hexadecimal byte string.'
        return util.pubkey_to_hash(pubkey).encode('hex').upper()

    def q_checkaddress(abe, page, chain):
        """检查地址的有效性."""
        addr = wsgiref.util.shift_path_info(page['env'])
        if addr is None:
            return \
                "返回地址版本的十六进制字符串.\n" \
                "如果地址无效, 为兼容BBE，返回 X5, SZ, 或者 CK.\n" \
                "/q/checkaddress/ADDRESS\n"
        if util.possible_address(addr):
            version, hash = util.decode_address(addr)
            if util.hash_to_address(version, hash) == addr:
                return version.encode('hex').upper()
            return 'CK'
        if len(addr) >= 26:
            return 'X5'
        return 'SZ'

    def q_hashrate(abe, page, chain):
        """显示最近 N 个区块的全网算力平均值(单位hashes/s，默认 N=1440[约为一天的数量])."""
        if chain is None:
            return '显示最近 N 个区块的全网算力平均值 (单位hashes/s，默认 N=1440[约为一天的数量).\n' \
                '/chain/CHAIN/q/hashrate[/N]\n'
        interval = path_info_int(page, 1440)
        start = 0 - interval
        stop = None

        if stop == 0:
            stop = None

        if interval < 0 and start != 0:
            return 'ERROR: Negative N!'

        if interval < 0 or start < 0 or (stop is not None and stop < 0):
            count = abe.get_max_block_height(chain)
            if start < 0:
                start += count
            if stop is not None and stop < 0:
                stop += count
            if interval < 0:
                interval = -interval
                start = count - (count / interval) * interval

        # Select every INTERVAL blocks from START to STOP.
        # Standard SQL lacks an "every Nth row" feature, so we
        # provide it with the help of a table containing the integers.
        # We don't need all integers, only as many as rows we want to
        # fetch.  We happen to have a table with the desired integers,
        # namely chain_candidate; its block_height column covers the
        # required range without duplicates if properly constrained.
        # That is the story of the second JOIN.

        if stop is not None:
            stop_ix = (stop - start) / interval

        rows = abe.store.selectall("""
            SELECT b.block_height,
                   (b.block_nTime+28800),
                   b.block_chain_work,
                   b.block_nBits
              FROM block b
              JOIN chain_candidate cc ON (cc.block_id = b.block_id)
              JOIN chain_candidate ints ON (
                       ints.chain_id = cc.chain_id
                   AND ints.in_longest = 1
                   AND ints.block_height * ? + ? = cc.block_height)
             WHERE cc.in_longest = 1
               AND cc.chain_id = ?""" + (
                "" if stop is None else """
               AND ints.block_height <= ?""") + """
             ORDER BY cc.block_height""",
                                   (interval, start, chain['id'])
                                   if stop is None else
                                   (interval, start, chain['id'], stop_ix))

        for row in rows:
            height, nTime, chain_work, nBits = row
            nTime            = float(nTime)
            nBits            = int(nBits)
            target           = util.calculate_target(nBits)
            difficulty       = util.target_to_difficulty(target)
            work             = util.target_to_work(target)
            chain_work       = abe.store.binout_int(chain_work) - work

            if row is not rows[0]:
                height           = int(height)
                interval_work    = chain_work - prev_chain_work
                avg_target       = util.work_to_target(interval_work / interval)
                #if avg_target == target - 1:
                #    avg_target = target
                interval_seconds = nTime - prev_nTime
                if interval_seconds <= 0:
                    nethash = 'Infinity'
                else:
                    nethash = "%.0f" % (interval_work / interval_seconds,)
                ret = "%s\n" % (nethash)

            prev_nTime, prev_chain_work = nTime, chain_work

        return ret

    def q_nethash(abe, page, chain):
        """显示全网算力和难度的统计信息."""
        #chain = None
        if chain is None:
            return '每 INTERVAL 个区块显示一次统计信息.\n' \
                '负数表示从最后一个区块往前.\n' \
                '/chain/CHAIN/q/nethash[/INTERVAL[/START[/STOP]]]\n'
        interval = path_info_int(page, 144)
        start = path_info_int(page, 0)
        stop = path_info_int(page, None)

        #if interval < 144:
        #    return "Sorry, INTERVAL too low."

        if stop == 0:
            stop = None

        if interval < 0 and start != 0:
            return 'ERROR: Negative INTERVAL requires 0 START.'

        if interval < 0 or start < 0 or (stop is not None and stop < 0):
            count = abe.get_max_block_height(chain)
            if start < 0:
                start += count
            if stop is not None and stop < 0:
                stop += count
            if interval < 0:
                interval = -interval
                start = count - (count / interval) * interval

        # Select every INTERVAL blocks from START to STOP.
        # Standard SQL lacks an "every Nth row" feature, so we
        # provide it with the help of a table containing the integers.
        # We don't need all integers, only as many as rows we want to
        # fetch.  We happen to have a table with the desired integers,
        # namely chain_candidate; its block_height column covers the
        # required range without duplicates if properly constrained.
        # That is the story of the second JOIN.

        if stop is not None:
            stop_ix = (stop - start) / interval

        rows = abe.store.selectall("""
            SELECT b.block_height,
                   (b.block_nTime+28800),
                   b.block_chain_work,
                   b.block_nBits
              FROM block b
              JOIN chain_candidate cc ON (cc.block_id = b.block_id)
              JOIN chain_candidate ints ON (
                       ints.chain_id = cc.chain_id
                   AND ints.in_longest = 1
                   AND ints.block_height * ? + ? = cc.block_height)
             WHERE cc.in_longest = 1
               AND cc.chain_id = ?""" + (
                "" if stop is None else """
               AND ints.block_height <= ?""") + """
             ORDER BY cc.block_height""",
                                   (interval, start, chain['id'])
                                   if stop is None else
                                   (interval, start, chain['id'], stop_ix))
        ret = NETHASH_HEADER

        for row in rows:
            height, nTime, chain_work, nBits = row
            nTime            = float(nTime)
            nBits            = int(nBits)
            target           = util.calculate_target(nBits)
            difficulty       = util.target_to_difficulty(target)
            work             = util.target_to_work(target)
            chain_work       = abe.store.binout_int(chain_work) - work

            if row is not rows[0]:
                height           = int(height)
                interval_work    = chain_work - prev_chain_work
                avg_target       = util.work_to_target(interval_work / interval)
                #if avg_target == target - 1:
                #    avg_target = target
                interval_seconds = nTime - prev_nTime
                if interval_seconds <= 0:
                    nethash = 'Infinity'
                else:
                    nethash = "%.0f" % (interval_work / interval_seconds,)
                ret += "%d,%d,%d,%d,%.3f,%d,%.0f,%s\n" % (
                    height, nTime, target, avg_target, difficulty, work,
                    interval_seconds / interval, nethash)

            prev_nTime, prev_chain_work = nTime, chain_work

        return ret

    def q_totalbc(abe, page, chain):
        """显示全网发行币总量."""
        if chain is None:
            return '显示全网发行币总量.\n' \
                '/chain/CHAIN/q/totalbc[/HEIGHT]\n'
        height = path_info_uint(page, None)
        if height is None:
            row = abe.store.selectrow("""
                SELECT b.block_total_satoshis
                  FROM chain c
                  LEFT JOIN block b ON (c.chain_last_block_id = b.block_id)
                 WHERE c.chain_id = ?
            """, (chain['id'],))
        else:
            row = abe.store.selectrow("""
                SELECT b.block_total_satoshis
                  FROM chain_candidate cc
                  LEFT JOIN block b ON (b.block_id = cc.block_id)
                 WHERE cc.chain_id = ?
                   AND cc.block_height = ?
                   AND cc.in_longest = 1
            """, (chain['id'], height))
            if not row:
                return 'ERROR: block %d not seen yet' % (height,)
        return format_satoshis(row[0], chain) if row else 0

    def q_getusedaddrcount(abe, page, chain):
	"""获取使用的地址总数"""
	if chain is None:
		return '获取全网使用的地址总数(包括发送和接收)\n' \
			'/chain/CHAIN/q/getusedaddrcount\n'
	sql = """SELECT COUNT(pubkey.pubkey_id) FROM pubkey"""
	row = abe.store.selectrow(sql)
	ret = row[0]
	return ret

    def q_gettop100receivingaddresses(abe, page, chain):
	"""获取接收地址top100"""
	if chain is None:
		return '返回接收地址top100\n' \
			'/chan/CHAIN/q/gettop100receivingaddresses\n'
	sql = """SELECT pubkey_hash, SUM(txout_value) AS SatoshisReceived FROM txout_detail WHERE in_longest=1 AND pubkey_hash IS NOT NULL GROUP BY pubkey_hash ORDER BY SatoshisReceived DESC LIMIT 100;"""
	rows = abe.store.selectall(sql)
	version = chain['address_version']
	ret = "address,received\n"
	for row in rows:
		hash, coins = row
		try:
			hash = hash.decode('hex')
			ret += "%s,%.8f\n" % (util.hash_to_address(version, hash), coins/10**6)
		except:
			return "ERROR: invalid hash\n"
	return ret

    def q_gettop100sendingaddresses(abe, page, chain):
	"""获取发送地址top100"""
	if chain is None:
		return '返回发送地址top100\n' \
			'/chan/CHAIN/q/gettop100sendingaddresses\n'
	sql = """SELECT pubkey_hash, SUM(txin_value) AS SatoshisSent FROM txin_detail WHERE in_longest=1 AND prevout_id IS NOT NULL GROUP BY pubkey_hash ORDER BY SatoshisSent DESC LIMIT 100;
"""
	rows = abe.store.selectall(sql)
	version = chain['address_version']
	ret = "address,sent\n"
	for row in rows:
		hash, coins = row
		try:
			hash = hash.decode('hex')
			ret += "%s,%.8f\n" % (util.hash_to_address(version, hash), coins/10**6)
		except:
			return "ERROR: invalid hash\n"
	return ret

    def q_gettop100balances(abe, page, chain):
	"""获取地址余额top100"""
	if chain is None:
		return '返回地址余额top100\n' \
			'/chain/CHAIN/q/gettop100balances\n'
	sql = """SELECT pubkey_hash FROM pubkey;"""
	rows = abe.store.selectall(sql)
	version = chain['address_version']
	ret = "addr,balance\n"	
	result = []
	for row in rows:
		pubkey_hash = row[0]
		sql = """
	            SELECT COALESCE(SUM(txout.txout_value), 0)
	              FROM pubkey
	              JOIN txout ON txout.pubkey_id=pubkey.pubkey_id
	              JOIN block_tx ON block_tx.tx_id=txout.tx_id
	              JOIN block b ON b.block_id=block_tx.block_id
	              JOIN chain_candidate cc ON cc.block_id=b.block_id
	              WHERE
	                  pubkey.pubkey_hash = ? AND
	                  cc.chain_id = ? AND
	                  cc.in_longest = 1"""
		total_received = abe.store.selectrow(sql, (pubkey_hash, chain['id']))
		sql = """
	            SELECT COALESCE(SUM(txout.txout_value), 0)
	              FROM pubkey
	              JOIN txout ON txout.pubkey_id=pubkey.pubkey_id
	              JOIN txin ON txin.txout_id=txout.txout_id
	              JOIN block_tx ON block_tx.tx_id=txout.tx_id
	              JOIN block b ON b.block_id=block_tx.block_id
	              JOIN chain_candidate cc ON cc.block_id=b.block_id
	              WHERE
	                  pubkey.pubkey_hash = ? AND
	                  cc.chain_id = ? AND
	                  cc.in_longest = 1"""
		total_sent = abe.store.selectrow(sql, (pubkey_hash, chain['id']))
		final_balance = (total_received[0] - total_sent[0]) / 10**6
		hash = pubkey_hash.decode('hex')		
		result.append({'address':util.hash_to_address(version, hash),'balance':final_balance})
		

	sresult = sorted(result,key=lambda x:x['balance'],reverse=True)
	for i in range(10):	    
	    ret += "%s,%.8f\n" % (sresult[i]['address'], sresult[i]['balance'])
	return ret
	          
    def q_getbalance(abe, page, chain):
        """获取余额"""
        addr = wsgiref.util.shift_path_info(page['env'])
        if chain is None or addr is None:
            return '返回一个地址的余额\n' \
                '/chain/CHAIN/q/getbalance/ADDRESS\n'

        if not util.possible_address(addr):
            return 'ERROR: address invalid'

        version, hash = util.decode_address(addr)
        sql = """
            SELECT COALESCE(SUM(txout.txout_value), 0)
              FROM pubkey
              JOIN txout ON txout.pubkey_id=pubkey.pubkey_id
              JOIN block_tx ON block_tx.tx_id=txout.tx_id
              JOIN block b ON b.block_id=block_tx.block_id
              JOIN chain_candidate cc ON cc.block_id=b.block_id
              WHERE
                  pubkey.pubkey_hash = ? AND
                  cc.chain_id = ? AND
                  cc.in_longest = 1"""
        row = abe.store.selectrow(
            sql, (abe.store.binin(hash), chain['id']))
        ret = float(format_satoshis(row[0], chain));

        sql = """
            SELECT COALESCE(SUM(txout.txout_value), 0)
              FROM pubkey
              JOIN txout ON txout.pubkey_id=pubkey.pubkey_id
              JOIN txin ON txin.txout_id=txout.txout_id
              JOIN block_tx ON block_tx.tx_id=txout.tx_id
              JOIN block b ON b.block_id=block_tx.block_id
              JOIN chain_candidate cc ON cc.block_id=b.block_id
              WHERE
                  pubkey.pubkey_hash = ? AND
                  cc.chain_id = ? AND
                  cc.in_longest = 1"""
        row = abe.store.selectrow(
            sql, (abe.store.binin(hash), chain['id']))
        ret -= float(format_satoshis(row[0], chain));

        return str(ret)

    def q_getreceivedbyaddress(abe, page, chain):
        """获取地址接收的币总量"""
        addr = wsgiref.util.shift_path_info(page['env'])
        if chain is None or addr is None:
            return '返回该地址接收的币总量 (并非余额, 并没有见到发送的币总量)\n' \
                '/chain/CHAIN/q/getreceivedbyaddress/ADDRESS\n'

        if not util.possible_address(addr):
            return 'ERROR: address invalid'

        version, hash = util.decode_address(addr)
        sql = """
            SELECT COALESCE(SUM(txout.txout_value), 0)
              FROM pubkey
              JOIN txout ON txout.pubkey_id=pubkey.pubkey_id
              JOIN block_tx ON block_tx.tx_id=txout.tx_id
              JOIN block b ON b.block_id=block_tx.block_id
              JOIN chain_candidate cc ON cc.block_id=b.block_id
              WHERE
                  pubkey.pubkey_hash = ? AND
                  cc.chain_id = ? AND
                  cc.in_longest = 1"""
        row = abe.store.selectrow(
            sql, (abe.store.binin(hash), chain['id']))
        ret = format_satoshis(row[0], chain);

        return ret

    def q_getsentbyaddress(abe, page, chain):
        """获取发送的币总量"""
        addr = wsgiref.util.shift_path_info(page['env'])
        if chain is None or addr is None:
            return '返回该地址发送的币总量\n' \
                '/chain/CHAIN/q/getsentbyaddress/ADDRESS\n'

        if not util.possible_address(addr):
            return 'ERROR: address invalid'

        version, hash = util.decode_address(addr)
        sql = """
            SELECT COALESCE(SUM(txout.txout_value), 0)
              FROM pubkey
              JOIN txout ON txout.pubkey_id=pubkey.pubkey_id
              JOIN txin ON txin.txout_id=txout.txout_id
              JOIN block_tx ON block_tx.tx_id=txout.tx_id
              JOIN block b ON b.block_id=block_tx.block_id
              JOIN chain_candidate cc ON cc.block_id=b.block_id
              WHERE
                  pubkey.pubkey_hash = ? AND
                  cc.chain_id = ? AND
                  cc.in_longest = 1"""
        row = abe.store.selectrow(
            sql, (abe.store.binin(hash), chain['id']))
        ret = format_satoshis(row[0], chain);

        return ret

    def q_fb(abe, page, chain):
        """获取地址的 firstbits."""

        if not abe.store.use_firstbits:
            raise PageNotFound()

        addr = wsgiref.util.shift_path_info(page['env'])
        if addr is None:
            return '显示 ADDRESS\'s firstbits:' \
                ' 唯一的、首字母大写的、区分大小写的、最简短的子串\n' \
                ' 区别于其他首次出现的或在同一个区块内的地址\n' \
                '参见 http://firstbits.com/.\n' \
                '如果没有 firstbits，则返回空.\n' \
                '/chain/CHAIN/q/fb/ADDRESS\n' \
                '/q/fb/ADDRESS\n'

        if not util.possible_address(addr):
            return 'ERROR: address invalid'

        version, dbhash = util.decode_address(addr)
        ret = abe.store.get_firstbits(
            address_version = version,
            db_pubkey_hash = abe.store.binin(dbhash),
            chain_id = (chain and chain['id']))

        if ret is None:
            return 'ERROR: address not in the chain.'

        return ret

    def q_addr(abe, page, chain):
        """返回给定firstbits的地址."""

        if not abe.store.use_firstbits:
            raise PageNotFound()

        fb = wsgiref.util.shift_path_info(page['env'])
        if fb is None:
            return '返回给定 FIRSTBITS 的地址:' \
                ' CHAIN 中第一个以FIRSTBITS开头的匹配的地址,' \
                ' 对比区分大小写.\n' \
                '参见 http://firstbits.com/.\n' \
                '如果没有任何匹配，则返回给定参数.\n' \
                '/chain/CHAIN/q/addr/FIRSTBITS\n' \
                '/q/addr/FIRSTBITS\n'

        return "\n".join(abe.store.firstbits_to_addresses(
                fb, chain_id = (chain and chain['id'])))

    def handle_download(abe, page):
        name = abe.args.download_name
        if name is None:
            name = re.sub(r'\W+', '-', ABE_APPNAME.lower()) + '-' + ABE_VERSION
        fileobj = lambda: None
        fileobj.func_dict['write'] = page['start_response'](
            '200 OK',
            [('Content-type', 'application/x-gtar-compressed'),
             ('Content-disposition', 'filename=' + name + '.tar.gz')])
        import tarfile
        with tarfile.TarFile.open(fileobj=fileobj, mode='w|gz',
                                  format=tarfile.PAX_FORMAT) as tar:
            tar.add(os.path.split(__file__)[0], name)
        raise Streamed()

    def serve_static(abe, path, start_response):
        slen = len(abe.static_path)
        if path[:slen] != abe.static_path:
            raise PageNotFound()
        path = path[slen:]
        try:
            # Serve static content.
            # XXX Should check file modification time and handle HTTP
            # if-modified-since.  Or just hope serious users will map
            # our htdocs as static in their web server.
            # XXX is "+ '/' + path" adequate for non-POSIX systems?
            found = open(abe.htdocs + '/' + path, "rb")
            import mimetypes
            type, enc = mimetypes.guess_type(path)
            # XXX Should do something with enc if not None.
            # XXX Should set Content-length.
            start_response('200 OK', [('Content-type', type or 'text/plain')])
            return found
        except IOError:
            raise PageNotFound()

    # Change this if you want empty or multi-byte address versions.
    def is_address_version(abe, v):
        return len(v) == 1

    def short_link(abe, page, link):
        base = abe.base_url
        if base is None:
            env = page['env'].copy()
            env['SCRIPT_NAME'] = posixpath.normpath(
                posixpath.dirname(env['SCRIPT_NAME'] + env['PATH_INFO'])
                + '/' + page['dotdot'])
            env['PATH_INFO'] = link
            full = wsgiref.util.request_uri(env)
        else:
            full = base + link

        return ['<p class="shortlink">短链接: <a href="',
                page['dotdot'], link, '">', full, '</a></p>\n']

def find_htdocs():
    return os.path.join(os.path.split(__file__)[0], 'htdocs')

def get_int_param(page, name):
    vals = page['params'].get(name)
    return vals and int(vals[0])

def path_info_uint(page, default):
    ret = path_info_int(page, None)
    if ret is None or ret < 0:
        return default
    return ret

def path_info_int(page, default):
    s = wsgiref.util.shift_path_info(page['env'])
    if s is None:
        return default
    try:
        return int(s)
    except ValueError:
        return default

def format_time(nTime):
    import time
    return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(nTime)))

def format_satoshis(satoshis, chain):
    # XXX Should find COIN and LOG10COIN from chain.
    if satoshis is None:
        return ''
    if satoshis < 0:
        return '-' + format_satoshis(-satoshis, chain)
    satoshis = int(satoshis)
    integer = satoshis / COIN
    frac = satoshis % COIN
    return (str(integer) +
            ('.' + (('0' * LOG10COIN) + str(frac))[-LOG10COIN:])
            .rstrip('0').rstrip('.'))

def format_difficulty(diff):
    idiff = int(diff)
    ret = '.%03d' % (int(round((diff - idiff) * 1000)),)
    while idiff > 999:
        ret = (' %03d' % (idiff % 1000,)) + ret
        idiff = idiff / 1000
    return str(idiff) + ret

def hash_to_address_link(version, hash, dotdot):
    if hash == DataStore.NULL_PUBKEY_HASH:
        return 'Destroyed'
    if hash is None:
        return 'UNKNOWN'
    addr = util.hash_to_address(version, hash)
    return ['<a href="', dotdot, 'address/', addr, '">', addr, '</a>']

def decode_script(script):
    if script is None:
        return ''
    try:
        return deserialize.decode_script(script)
    except KeyError, e:
        return 'Nonstandard script'

def b58hex(b58):
    try:
        return base58.b58decode(b58, None).encode('hex_codec')
    except:
        raise PageNotFound()

def hexb58(hex):
    return base58.b58encode(hex.decode('hex_codec'))

def block_shortlink(block_hash):
    zeroes = 0
    for c in block_hash:
        if c == '0':
            zeroes += 1
        else:
            break
    zeroes &= ~1
    return hexb58("%02x%s" % (zeroes / 2, block_hash[zeroes : zeroes+12]))

def shortlink_block(link):
    try:
        data = base58.b58decode(link, None)
    except:
        raise PageNotFound()
    return ('00' * ord(data[0])) + data[1:].encode('hex_codec')

def is_hash_prefix(s):
    return HASH_PREFIX_RE.match(s) and len(s) >= HASH_PREFIX_MIN

def flatten(l):
    if isinstance(l, list):
        return ''.join(map(flatten, l))
    if l is None:
        raise Exception('NoneType in HTML conversion')
    if isinstance(l, unicode):
        return l
    return str(l)

def fix_path_info(env):
    pi = env['PATH_INFO']
    pi = posixpath.normpath(pi)
    if pi[-1:] != '/' and env['PATH_INFO'][-1:] == '/':
        pi += '/'
    if pi == env['PATH_INFO']:
        return False
    env['PATH_INFO'] = pi
    return True

def redirect(page):
    uri = wsgiref.util.request_uri(page['env'])
    page['start_response'](
        '301 Moved Permanently',
        [('Location', uri),
         ('Content-Type', 'text/html')])
    return ('<html><head><title>Moved</title></head>\n'
            '<body><h1>Moved</h1><p>This page has moved to '
            '<a href="' + uri + '">' + uri + '</a></body></html>')

def serve(store):
    args = store.args
    abe = Abe(store, args)
    if args.host or args.port:
        # HTTP server.
        if args.host is None:
            args.host = "localhost"
        from wsgiref.simple_server import make_server
        port = int(args.port or 80)
        httpd = make_server(args.host, port, abe)
        abe.log.warning("Listening on http://%s:%d", args.host, port)
        # httpd.shutdown() sometimes hangs, so don't call it.  XXX
        httpd.serve_forever()
    else:
        # FastCGI server.
        from flup.server.fcgi import WSGIServer

        # In the case where the web server starts Abe but can't signal
        # it on server shutdown (because Abe runs as a different user)
        # we arrange the following.  FastCGI script passes its pid as
        # --watch-pid=PID and enters an infinite loop.  We check every
        # minute whether it has terminated and exit when it has.
        wpid = args.watch_pid
        if wpid is not None:
            wpid = int(wpid)
            interval = 60.0  # XXX should be configurable.
            from threading import Timer
            import signal
            def watch():
                if not process_is_alive(wpid):
                    abe.log.warning("process %d terminated, exiting", wpid)
                    #os._exit(0)  # sys.exit merely raises an exception.
                    os.kill(os.getpid(), signal.SIGTERM)
                    return
                abe.log.log(0, "process %d found alive", wpid)
                Timer(interval, watch).start()
            Timer(interval, watch).start()
        WSGIServer(abe).run()

def process_is_alive(pid):
    # XXX probably fails spectacularly on Windows.
    import errno
    try:
        os.kill(pid, 0)
        return True
    except OSError, e:
        if e.errno == errno.EPERM:
            return True  # process exists, but we can't send it signals.
        if e.errno == errno.ESRCH:
            return False # no such process.
        raise

def main(argv):
    conf = {
        "port":                     None,
        "host":                     None,
        "no_serve":                 None,
        "debug":                    None,
        "static_path":              None,
        "document_root":            None,
        "auto_agpl":                None,
        "download_name":            None,
        "watch_pid":                None,
        "base_url":                 None,
        "logging":                  None,
        "address_history_rows_max": None,
        "shortlink_type":           None,

        "template":     DEFAULT_TEMPLATE,
        "template_vars": {
            "ABE_URL": ABE_URL,
            "APPNAME": ABE_APPNAME,
            "VERSION": ABE_VERSION,
            "COPYRIGHT": COPYRIGHT,
            "COPYRIGHT_YEARS": COPYRIGHT_YEARS,
            "COPYRIGHT_URL": COPYRIGHT_URL,
            "DONATIONS_BTC": DONATIONS_BTC,
            "DONATIONS_YBC": DONATIONS_YBC,
            "CONTENT_TYPE": DEFAULT_CONTENT_TYPE,
            },
        }
    conf.update(DataStore.CONFIG_DEFAULTS)

    args, argv = readconf.parse_argv(argv, conf)
    if not argv:
        pass
    elif argv[0] in ('-h', '--help'):
        print ("""Usage: python -m Abe.abe [-h] [--config=FILE] [--CONFIGVAR=VALUE]...

A Bitcoin block chain browser.

  --help                    Show this help message and exit.
  --version                 Show the program version and exit.
  --print-htdocs-directory  Show the static content directory name and exit.
  --config FILE             Read options from FILE.

All configuration variables may be given as command arguments.
See abe.conf for commented examples.""")
        return 0
    elif argv[0] in ('-v', '--version'):
        print ABE_APPNAME, ABE_VERSION
        print "Schema version", DataStore.SCHEMA_VERSION
        return 0
    elif argv[0] == '--print-htdocs-directory':
        print find_htdocs()
        return 0
    else:
        sys.stderr.write(
            "Error: unknown option `%s'\n"
            "See `python -m Abe.abe --help' for more information.\n"
            % (argv[0],))
        return 1

    logging.basicConfig(
        stream=sys.stdout,
        level=logging.DEBUG,
        format=DEFAULT_LOG_FORMAT)
    if args.logging is not None:
        import logging.config as logging_config
        logging_config.dictConfig(args.logging)

    if args.auto_agpl:
        import tarfile

    store = make_store(args)
    if (not args.no_serve):
        serve(store)
    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
