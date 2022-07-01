import requests
import pandas as pd


def scrape_asset_allocation(sec_id):
    url = f"https://www.us-api.morningstar.com/sal/sal-service/fund/process/asset/v2/{sec_id}/data"
    querystring = {"languageId":"en","locale":"en","clientId":"MDC_intl","benchmarkId":"mstarorcat","component":"sal-components-mip-asset-allocation","version":"3.60.0"}
    headers = {
        "authority": "www.us-api.morningstar.com",
        "accept": "*/*",
        "accept-language": "en-US,en-GB;q=0.9,en;q=0.8",
        "authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ik1EY3hOemRHTnpGRFJrSTRPRGswTmtaRU1FSkdOekl5TXpORFJrUTROemd6TWtOR016bEdOdyJ9.eyJodHRwczovL21vcm5pbmdzdGFyLmNvbS9tc3Rhcl9pZCI6Ijc2NjU2NkFELTkxMjEtNDJDMS05RjM2LTkwREM1RkNENUUxQyIsImh0dHBzOi8vbW9ybmluZ3N0YXIuY29tL2VtYWlsIjoiajYzazg5NW5qcGE4bW5tcTRwMmptc3R0Y2g5MHNhaGJAbWFhcy1tc3Rhci5jb20iLCJodHRwczovL21vcm5pbmdzdGFyLmNvbS9yb2xlIjpbIkVDLlNlcnZpY2UuQ29uZmlndXJhdGlvbiIsIkVDLlNlcnZpY2UuSG9zdGluZyIsIkVDVVMuQVBJLkF1dG9jb21wbGV0ZSIsIkVDVVMuQVBJLlNjcmVlbmVyIiwiRUNVUy5BUEkuU2VjdXJpdGllcyIsIlBBQVBJVjEuWHJheSIsIlZlbG9VSS5BbGxvd0FjY2VzcyJdLCJodHRwczovL21vcm5pbmdzdGFyLmNvbS9jb21wYW55X2lkIjoiMjgyY2M4NjUtMzUwNS00ZTIwLThkMjQtODRhNDM4YjIxOTI0IiwiaHR0cHM6Ly9tb3JuaW5nc3Rhci5jb20vaW50ZXJuYWxfY29tcGFueV9pZCI6IkNsaWVudDAiLCJodHRwczovL21vcm5pbmdzdGFyLmNvbS9kYXRhX3JvbGUiOlsiRUNVUy5EYXRhLlVTLk9wZW5FbmRGdW5kcyIsIlFTLk1hcmtldHMiLCJRUy5QdWxscXMiLCJTQUwuU2VydmljZSJdLCJodHRwczovL21vcm5pbmdzdGFyLmNvbS9sZWdhY3lfY29tcGFueV9pZCI6IjI0YmYwYTg1LTMyNzEtNGIxYi1hYjFlLTBlOWZkMTg4MThiZCIsImh0dHBzOi8vbW9ybmluZ3N0YXIuY29tL3VpbV9yb2xlcyI6Ik1VX01FTUJFUl8xXzEiLCJpc3MiOiJodHRwczovL2xvZ2luLXByb2QubW9ybmluZ3N0YXIuY29tLyIsInN1YiI6ImF1dGgwfDc2NjU2NkFELTkxMjEtNDJDMS05RjM2LTkwREM1RkNENUUxQyIsImF1ZCI6WyJodHRwczovL2F1dGgwLWF3c3Byb2QubW9ybmluZ3N0YXIuY29tL21hYXMiLCJodHRwczovL3VpbS1wcm9kLm1vcm5pbmdzdGFyLmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE2NTY2ODYzMjgsImV4cCI6MTY1NjY4OTkyOCwiYXpwIjoiaVFrV3hvYXBKOVB4bDhjR1pMeWFYWnNiWFY3OWc2NG0iLCJzY29wZSI6Im9wZW5pZCIsImd0eSI6InBhc3N3b3JkIn0.QolfNiRDmLFuCedLfSlMRpIPkOteNATQlGHt6RyVmswWw9CQi5H2fhA6Jd6dW-tXtA6rcXXLeKaFqfAWwYXvsl2m8Bm3vPclZ_dUDSaEAavz0nLiVhclKfFpH6vKo_K2oX--KkI60kS3T0dHeJ7WHwx4FuGbPVKwGOaxVdl55jqWraFaHWGPFL7ZSkxIb1Pljwrp1BSK9W9U-GiAQCW-u32WkxWeLIjQm71cLPvxl9G8LrsVsC1Uw3MC-MMHAnyDZuKM4mZoJsUG8NJNYho0gH-JzQ8uYgJFKt9k5_k4cD-ZiOaRpc5WXtdlZj6BEHTf5gARa0O83B05Wqry7dsYFA",
        "credentials": "omit",
        "origin": "https://www.morningstar.hk",
        "referer": "https://www.morningstar.hk/hk/report/fund/portfolio.aspx?t=0P0001O7IE&fundservcode=&lang=en-HK",
        "sec-ch-ua": "^\^.Not/A",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "^\^Windows^^",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "cross-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
        "x-api-realtime-e": "eyJlbmMiOiJBMTI4R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.jidbnhWUJvWMvD2omPxv9L_-iiW424c89SMUexsXDrY3VE6CfIU1g6cRPZ-DToeSbtFEvMPV4DoNlOtzUJ63Ryja23rtjlMsrpV-nLpUUpjcZp7ZL0YjGQNbsq1a-vAwf7GBOk6lnsOWycXB0mKaHMXfHpgwAsRcfGK1QpIb27U.da0qHJBnRmc0_EOq.1fXioE66EIItzsggPK3b4HNypNp1Ltva84HWRNmxwBPsUo5kvUXaYDuFjLHT2K39RsoMqZzERuQMfrP8fYoqTfkBmg-xD5sQbqHYeKgvuqMdOlkRVx4y16ft1RiliknyWNGxTd_5KXZrkDadGR7gHQyC775iNEBi0bI9F_JXu3t7_8uBMHNOYlIeRHm2Dqmz17ukL_zoGOX74_KiaYkBL5RamA.7wz-dbBZR15KCDRanQrHPg",
        "x-api-requestid": "40d24b45-53d4-af00-e570-9ef6df8544dc",
        "x-sal-contenttype": "e7FDDltrTy+tA2HnLovvGL0LFMwT+KkEptGju5wXVTU="
    }
    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
    data = response.json()

    res = []
    asset_classes = ['assetAllocPreferred', 'assetAllocOther', 'assetAllocEquity', 'assetAllocCash', 'assetAllocConvertible', 'assetAllocFixedIncome']
    for a in asset_classes:
        allocation_weightings = data['globalAllocationMap'][a].values()
        allocation_names = data['globalAllocationMap'][a].keys()
        allocations = pd.DataFrame(allocation_weightings, index=allocation_names).T
        res.append(allocations)

    res = pd.concat(res)
    res['assetClass'] = ['Preferred', 'Other', 'Equity', 'Cash', 'Convertible', 'FixedIncome']
    res['assetName'] = data['fundName']
    res['date'] = data['portfolioDateCategoryGlobal']
    res['secId'] = sec_id

    return res


def scrape_bond_breakdown(sec_id):
    url = f"https://www.us-api.morningstar.com/sal/sal-service/fund/portfolio/creditQuality/{sec_id}/data"
    querystring = {"languageId":"en","locale":"en","clientId":"MDC_intl","benchmarkId":"mstarorcat","component":"sal-components-mip-credit-quality","version":"3.60.0"}
    headers = {
        "authority": "www.us-api.morningstar.com",
        "accept": "*/*",
        "accept-language": "en-US,en-GB;q=0.9,en;q=0.8",
        "authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ik1EY3hOemRHTnpGRFJrSTRPRGswTmtaRU1FSkdOekl5TXpORFJrUTROemd6TWtOR016bEdOdyJ9.eyJodHRwczovL21vcm5pbmdzdGFyLmNvbS9tc3Rhcl9pZCI6Ijc2NjU2NkFELTkxMjEtNDJDMS05RjM2LTkwREM1RkNENUUxQyIsImh0dHBzOi8vbW9ybmluZ3N0YXIuY29tL2VtYWlsIjoiajYzazg5NW5qcGE4bW5tcTRwMmptc3R0Y2g5MHNhaGJAbWFhcy1tc3Rhci5jb20iLCJodHRwczovL21vcm5pbmdzdGFyLmNvbS9yb2xlIjpbIkVDLlNlcnZpY2UuQ29uZmlndXJhdGlvbiIsIkVDLlNlcnZpY2UuSG9zdGluZyIsIkVDVVMuQVBJLkF1dG9jb21wbGV0ZSIsIkVDVVMuQVBJLlNjcmVlbmVyIiwiRUNVUy5BUEkuU2VjdXJpdGllcyIsIlBBQVBJVjEuWHJheSIsIlZlbG9VSS5BbGxvd0FjY2VzcyJdLCJodHRwczovL21vcm5pbmdzdGFyLmNvbS9jb21wYW55X2lkIjoiMjgyY2M4NjUtMzUwNS00ZTIwLThkMjQtODRhNDM4YjIxOTI0IiwiaHR0cHM6Ly9tb3JuaW5nc3Rhci5jb20vaW50ZXJuYWxfY29tcGFueV9pZCI6IkNsaWVudDAiLCJodHRwczovL21vcm5pbmdzdGFyLmNvbS9kYXRhX3JvbGUiOlsiRUNVUy5EYXRhLlVTLk9wZW5FbmRGdW5kcyIsIlFTLk1hcmtldHMiLCJRUy5QdWxscXMiLCJTQUwuU2VydmljZSJdLCJodHRwczovL21vcm5pbmdzdGFyLmNvbS9sZWdhY3lfY29tcGFueV9pZCI6IjI0YmYwYTg1LTMyNzEtNGIxYi1hYjFlLTBlOWZkMTg4MThiZCIsImh0dHBzOi8vbW9ybmluZ3N0YXIuY29tL3VpbV9yb2xlcyI6Ik1VX01FTUJFUl8xXzEiLCJpc3MiOiJodHRwczovL2xvZ2luLXByb2QubW9ybmluZ3N0YXIuY29tLyIsInN1YiI6ImF1dGgwfDc2NjU2NkFELTkxMjEtNDJDMS05RjM2LTkwREM1RkNENUUxQyIsImF1ZCI6WyJodHRwczovL2F1dGgwLWF3c3Byb2QubW9ybmluZ3N0YXIuY29tL21hYXMiLCJodHRwczovL3VpbS1wcm9kLm1vcm5pbmdzdGFyLmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE2NTY2ODYzMjgsImV4cCI6MTY1NjY4OTkyOCwiYXpwIjoiaVFrV3hvYXBKOVB4bDhjR1pMeWFYWnNiWFY3OWc2NG0iLCJzY29wZSI6Im9wZW5pZCIsImd0eSI6InBhc3N3b3JkIn0.QolfNiRDmLFuCedLfSlMRpIPkOteNATQlGHt6RyVmswWw9CQi5H2fhA6Jd6dW-tXtA6rcXXLeKaFqfAWwYXvsl2m8Bm3vPclZ_dUDSaEAavz0nLiVhclKfFpH6vKo_K2oX--KkI60kS3T0dHeJ7WHwx4FuGbPVKwGOaxVdl55jqWraFaHWGPFL7ZSkxIb1Pljwrp1BSK9W9U-GiAQCW-u32WkxWeLIjQm71cLPvxl9G8LrsVsC1Uw3MC-MMHAnyDZuKM4mZoJsUG8NJNYho0gH-JzQ8uYgJFKt9k5_k4cD-ZiOaRpc5WXtdlZj6BEHTf5gARa0O83B05Wqry7dsYFA",
        "credentials": "omit",
        "origin": "https://www.morningstar.hk",
        "referer": "https://www.morningstar.hk/hk/report/fund/portfolio.aspx?t=0P0001O7IE&fundservcode=&lang=en-HK",
        "sec-ch-ua": "^\^.Not/A",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "^\^Windows^^",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "cross-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
        "x-api-realtime-e": "eyJlbmMiOiJBMTI4R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.jidbnhWUJvWMvD2omPxv9L_-iiW424c89SMUexsXDrY3VE6CfIU1g6cRPZ-DToeSbtFEvMPV4DoNlOtzUJ63Ryja23rtjlMsrpV-nLpUUpjcZp7ZL0YjGQNbsq1a-vAwf7GBOk6lnsOWycXB0mKaHMXfHpgwAsRcfGK1QpIb27U.da0qHJBnRmc0_EOq.1fXioE66EIItzsggPK3b4HNypNp1Ltva84HWRNmxwBPsUo5kvUXaYDuFjLHT2K39RsoMqZzERuQMfrP8fYoqTfkBmg-xD5sQbqHYeKgvuqMdOlkRVx4y16ft1RiliknyWNGxTd_5KXZrkDadGR7gHQyC775iNEBi0bI9F_JXu3t7_8uBMHNOYlIeRHm2Dqmz17ukL_zoGOX74_KiaYkBL5RamA.7wz-dbBZR15KCDRanQrHPg",
        "x-api-requestid": "9e2b4797-bc54-42bf-2fe2-0fb45262b430",
        "x-sal-contenttype": "e7FDDltrTy+tA2HnLovvGL0LFMwT+KkEptGju5wXVTU="
    }

    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
    data = response.json()
    data['secId'] = sec_id

    return data


for i in sec_ids:
    asset_allocation = scrape_asset_allocation(i)
    bond_breakdown = scrape_bond_breakdown(i)
    df = pd.merge(asset_allocation, bond_breakdown, on='secId')
    df.to_csv(f'{i}.csv', index=False)
